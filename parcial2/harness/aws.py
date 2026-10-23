"""AWS adapter used by the tests. RealAWS wraps boto3; the selftest injects a simulated one.

Every method returns plain dicts/lists so the tests never touch boto3 objects. Methods that need
a permission the Learner Lab may deny return None ("could not determine"), and the tests report
that as a warning instead of a silent pass.
"""
import datetime as dt
import time
from urllib.parse import urlparse


def _utc(x):
    if x is None:
        return None
    if x.tzinfo is None:
        return x.replace(tzinfo=dt.timezone.utc)
    return x.astimezone(dt.timezone.utc)


def split_s3(uri: str):
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


class RealAWS:
    def __init__(self, region="us-east-1", athena_output=None):
        import boto3
        self.session = boto3.Session(region_name=region)
        self.athena_output = athena_output

    def _c(self, name):
        return self.session.client(name)

    # ---------------------------------------------------------------- identity
    def account_id(self):
        try:
            return self._c("sts").get_caller_identity()["Account"]
        except Exception:
            return None

    # ------------------------------------------------------------------ Athena
    def athena_run(self, sql, database, workgroup, timeout=180):
        a = self._c("athena")
        kw = dict(QueryString=sql, QueryExecutionContext={"Database": database}, WorkGroup=workgroup)
        if self.athena_output:
            kw["ResultConfiguration"] = {"OutputLocation": self.athena_output}
        qid = a.start_query_execution(**kw)["QueryExecutionId"]
        t0 = time.time()
        while True:
            qe = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = qe["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() - t0 > timeout:
                raise TimeoutError(f"Athena query {qid} still {state} after {timeout}s")
            time.sleep(1.0)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query {qid} {state}: {qe['Status'].get('StateChangeReason')}")
        rows, first = [], True
        for page in a.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            for r in page["ResultSet"]["Rows"]:
                if first:          # for SELECT statements the first row is the column header
                    first = False
                    continue
                rows.append([c.get("VarCharValue") for c in r["Data"]])
        return {"execution_id": qid, "rows": rows,
                "data_scanned_bytes": int(qe.get("Statistics", {}).get("DataScannedInBytes", 0))}

    def athena_list_executions(self, workgroup, limit=50):
        a = self._c("athena")
        ids = a.list_query_executions(WorkGroup=workgroup, MaxResults=min(limit, 50))["QueryExecutionIds"]
        if not ids:
            return []
        out = []
        for qe in a.batch_get_query_execution(QueryExecutionIds=ids)["QueryExecutions"]:
            out.append({"id": qe["QueryExecutionId"], "state": qe["Status"]["State"], "query": qe.get("Query", ""),
                        "data_scanned_bytes": int(qe.get("Statistics", {}).get("DataScannedInBytes", 0)),
                        "submitted_at": _utc(qe["Status"].get("SubmissionDateTime"))})
        return out

    # -------------------------------------------------------------------- Glue
    def glue_get_table(self, database, name):
        g = self._c("glue")
        try:
            t = g.get_table(DatabaseName=database, Name=name)["Table"]
        except g.exceptions.EntityNotFoundException:
            return None
        sd = t.get("StorageDescriptor", {})
        params = t.get("Parameters", {})
        return {"name": t["Name"],
                "columns": [{"name": c["Name"], "type": c["Type"].lower()} for c in sd.get("Columns", [])],
                "partition_keys": [{"name": c["Name"], "type": c["Type"].lower()} for c in t.get("PartitionKeys", [])],
                "input_format": sd.get("InputFormat", ""),
                "serde": (sd.get("SerdeInfo") or {}).get("SerializationLibrary", ""),
                "location": sd.get("Location", ""),
                "classification": params.get("classification", ""),
                "projection_enabled": str(params.get("projection.enabled", "false")).lower() == "true"}

    def glue_partition_count(self, database, name):
        g = self._c("glue")
        n = 0
        for page in g.get_paginator("get_partitions").paginate(DatabaseName=database, TableName=name):
            n += len(page["Partitions"])
        return n

    # --------------------------------------------------------------------- S3
    def s3_list(self, uri):
        bucket, prefix = split_s3(uri)
        s3 = self._c("s3")
        out = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                out.append({"key": o["Key"], "size": o["Size"], "last_modified": _utc(o["LastModified"])})
        return out

    # --------------------------------------------------------------------- EMR
    def compute_runs(self, days=45):
        """Normalised list of EMR runs (EC2 steps and EMR Serverless job runs)."""
        runs = []
        since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
        try:
            emr = self._c("emr")
            for page in emr.get_paginator("list_clusters").paginate(CreatedAfter=since):
                for c in page["Clusters"]:
                    for sp in emr.get_paginator("list_steps").paginate(ClusterId=c["Id"]):
                        for s in sp["Steps"]:
                            tl = s["Status"].get("Timeline", {})
                            runs.append({"kind": "emr-ec2", "id": s["Id"], "cluster_id": c["Id"],
                                         "state": s["Status"]["State"],
                                         "started_at": _utc(tl.get("StartDateTime")),
                                         "ended_at": _utc(tl.get("EndDateTime"))})
        except Exception:
            pass
        try:
            sv = self._c("emr-serverless")
            for app in sv.get_paginator("list_applications").paginate().build_full_result()["applications"]:
                for jr in sv.get_paginator("list_job_runs").paginate(applicationId=app["id"]).build_full_result()["jobRuns"]:
                    runs.append({"kind": "emr-serverless", "id": jr["id"], "cluster_id": app["id"],
                                 "state": "COMPLETED" if jr["state"] == "SUCCESS" else jr["state"],
                                 "started_at": _utc(jr.get("createdAt")), "ended_at": _utc(jr.get("updatedAt"))})
        except Exception:
            pass
        return runs

    def emr_usage(self, days=45):
        """{'active_clusters': [...], 'started_serverless_apps': n, 'normalized_instance_hours': x,
            'instance_types': [...]}  or None when EMR cannot be queried."""
        try:
            emr = self._c("emr")
            since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
            active, hours, types = [], 0.0, set()
            for page in emr.get_paginator("list_clusters").paginate(CreatedAfter=since):
                for c in page["Clusters"]:
                    d = emr.describe_cluster(ClusterId=c["Id"])["Cluster"]
                    hours += float(d.get("NormalizedInstanceHours", 0))
                    if c["Status"]["State"] in ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"):
                        active.append(c["Id"])
                    for ig in emr.list_instance_groups(ClusterId=c["Id"])["InstanceGroups"]:
                        types.add(ig["InstanceType"])
            started = 0
            try:
                sv = self._c("emr-serverless")
                for app in sv.get_paginator("list_applications").paginate().build_full_result()["applications"]:
                    if app["state"] == "STARTED":
                        started += 1
            except Exception:
                pass
            return {"active_clusters": active, "started_serverless_apps": started,
                    "normalized_instance_hours": hours, "instance_types": sorted(types)}
        except Exception:
            return None

    # ------------------------------------------------------- billable resources
    def rds_instances(self):
        try:
            return [{"id": d["DBInstanceIdentifier"], "cls": d["DBInstanceClass"], "multi_az": bool(d.get("MultiAZ")),
                     "engine": d.get("Engine")} for d in self._c("rds").describe_db_instances()["DBInstances"]]
        except Exception:
            return None

    def ec2_instances(self):
        try:
            out = []
            for page in self._c("ec2").get_paginator("describe_instances").paginate(
                    Filters=[{"Name": "instance-state-name", "Values": ["pending", "running"]}]):
                for r in page["Reservations"]:
                    for i in r["Instances"]:
                        out.append({"id": i["InstanceId"], "type": i["InstanceType"], "state": i["State"]["Name"]})
            return out
        except Exception:
            return None

    def nat_gateways(self):
        try:
            return [n["NatGatewayId"] for n in self._c("ec2").describe_nat_gateways(
                Filter=[{"Name": "state", "Values": ["pending", "available"]}])["NatGateways"]]
        except Exception:
            return None

    def load_balancers(self):
        try:
            return [lb["LoadBalancerArn"] for lb in self._c("elbv2").describe_load_balancers()["LoadBalancers"]]
        except Exception:
            return None

    def other_billable(self):
        """{service: count} for services that cost money when merely existing. Services the
        account cannot list are omitted (the test reports how many could not be determined)."""
        probes = {
            "ElastiCache": lambda: len(self._c("elasticache").describe_cache_clusters()["CacheClusters"]),
            "DynamoDB": lambda: len(self._c("dynamodb").list_tables()["TableNames"]),
            "Lambda": lambda: len(self._c("lambda").list_functions()["Functions"]),
            "ECS": lambda: len(self._c("ecs").list_clusters()["clusterArns"]),
            "EKS": lambda: len(self._c("eks").list_clusters()["clusters"]),
            "CloudFront": lambda: len((self._c("cloudfront").list_distributions().get("DistributionList") or {}).get("Items", []) or []),
            "Kinesis": lambda: len(self._c("kinesis").list_streams()["StreamNames"]),
            "APIGateway": lambda: len(self._c("apigateway").get_rest_apis()["items"]),
        }
        out, unknown = {}, []
        for name, fn in probes.items():
            try:
                n = fn()
                if n:
                    out[name] = n
            except Exception:
                unknown.append(name)
        return {"counts": out, "unknown": unknown}
