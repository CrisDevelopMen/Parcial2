"""Cost helpers: estimate the monthly cost of what is actually deployed, and read the student's
declared projection (cost_projection.yaml)."""
import os

import yaml


def estimate_monthly_cost(aws, pricing: dict, athena_bytes_per_month: float = 0.0, s3_gb: float = 0.0):
    """Returns (total_usd, breakdown_dict, unknown_list). Only always-on resources plus the EMR job
    hours already consumed are counted; this is an estimate to cross-check the student's numbers,
    not an invoice."""
    hpm = pricing.get("hours_per_month", 730)
    unknown, items = [], {}

    rds = aws.rds_instances()
    if rds is None:
        unknown.append("rds")
    else:
        c = 0.0
        for d in rds:
            price = pricing["rds_per_hour"].get(d["cls"], pricing["default_unknown_instance_per_hour"])
            c += price * hpm * (2 if d["multi_az"] else 1)
        items["RDS"] = round(c, 2)

    ec2 = aws.ec2_instances()
    if ec2 is None:
        unknown.append("ec2")
    else:
        items["EC2"] = round(sum(pricing["ec2_per_hour"].get(i["type"], pricing["default_unknown_instance_per_hour"]) * hpm
                                 for i in ec2), 2)

    nat = aws.nat_gateways()
    if nat:
        items["NAT"] = round(len(nat) * pricing["nat_gateway_per_hour"] * hpm, 2)
    lbs = aws.load_balancers()
    if lbs:
        items["ELB"] = round(len(lbs) * pricing["load_balancer_per_hour"] * hpm, 2)

    emr = aws.emr_usage()
    if emr is None:
        unknown.append("emr")
    else:
        items["EMR"] = round(emr["normalized_instance_hours"] * pricing["emr_per_normalized_instance_hour"], 2)

    items["Athena"] = round(athena_bytes_per_month / 1e12 * pricing["athena_per_tb_scanned"], 2)
    items["S3"] = round(s3_gb * pricing["s3_per_gb_month"], 2)
    return round(sum(items.values()), 2), items, unknown


def load_declared(path):
    """Returns (data, error). The harness recomputes the total from line items."""
    if not path or not os.path.exists(path):
        return None, f"{path} not found"
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    items = data.get("line_items") or []
    total = round(sum(float(i.get("monthly_usd", 0)) for i in items), 2)
    data["_computed_total"] = total
    return data, None
