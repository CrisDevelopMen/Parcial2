"""R10-R12: leftover resources, simple architecture, cost projection."""
import pytest

from harness import report
from harness.cost import estimate_monthly_cost, load_declared


@pytest.mark.req("R10")
def test_no_running_emr(aws):
    u = aws.emr_usage()
    if u is None:
        pytest.skip("EMR cannot be queried with these credentials: NOT VERIFIED")
    report.note("R10", f"active clusters={u['active_clusters']} started serverless apps={u['started_serverless_apps']}")
    assert not u["active_clusters"], f"EMR clusters still active: {u['active_clusters']}"
    assert u["started_serverless_apps"] == 0, "EMR Serverless application left in STARTED state"


@pytest.mark.req("R10")
def test_emr_hours_cap(aws, thr):
    u = aws.emr_usage()
    if u is None:
        pytest.skip("EMR cannot be queried: NOT VERIFIED")
    cap = thr["cost"]["max_emr_normalized_instance_hours"]
    report.note("R10", f"normalized instance hours used: {u['normalized_instance_hours']} (cap {cap})")
    assert u["normalized_instance_hours"] <= cap, f"{u['normalized_instance_hours']} h > cap {cap}"


@pytest.mark.req("R11")
def test_instance_classes(aws, thr):
    c = thr["cost"]
    rds, ec2, emr = aws.rds_instances(), aws.ec2_instances(), aws.emr_usage()
    if rds is None and ec2 is None:
        pytest.skip("cannot list RDS/EC2: NOT VERIFIED")
    bad = [d["cls"] for d in (rds or []) if d["cls"] not in c["allowed_rds_classes"]]
    bad += [i["type"] for i in (ec2 or []) if i["type"] not in c["allowed_ec2_types"]]
    bad += [t for t in (emr or {}).get("instance_types", []) if t not in c["allowed_emr_instance_types"]]
    assert not bad, f"instance classes not allowed: {sorted(set(bad))}"
    if c["forbid_multi_az"]:
        assert not [d["id"] for d in (rds or []) if d["multi_az"]], "Multi-AZ RDS is not allowed"


@pytest.mark.req("R11")
def test_no_nat_and_service_count(aws, thr):
    c = thr["cost"]
    nat = aws.nat_gateways()
    if nat is None:
        pytest.skip("cannot list NAT gateways: NOT VERIFIED")
    if c["forbid_nat_gateway"]:
        assert not nat, f"NAT Gateway present: {nat}"
    lbs = aws.load_balancers()
    if lbs and c["warn_load_balancer"]:
        report.warn("R11", f"{len(lbs)} load balancer(s): about US$16/month each; justify it")
    other = aws.other_billable()
    services = set(other["counts"])
    if aws.rds_instances():
        services.add("RDS")
    if aws.ec2_instances():
        services.add("EC2")
    if lbs:
        services.add("ELB")
    services |= {"S3", "Athena", "Glue", "EMR"}   # required by the project
    if other["unknown"]:
        report.warn("R11", f"could not inspect: {other['unknown']} (not counted)")
    report.note("R11", f"billable services in use: {sorted(services)}")
    assert len(services) <= c["max_distinct_billable_services"], \
        f"{len(services)} billable services > {c['max_distinct_billable_services']}: {sorted(services)}"


@pytest.mark.req("R12")
def test_projection_file(cfg, thr):
    d, err = load_declared(cfg.get("cost_projection_file"))
    assert d, err
    assert d["line_items"], "cost_projection.yaml has no line_items"
    total = d["_computed_total"]
    ref, ratio = thr["cost"]["reference_monthly_usd"], thr["cost"]["max_ratio_vs_reference"]
    limit = min(ref * ratio, thr["cost"]["budget_usd"])
    report.note("R12", f"declared total {total} USD/month; limit {limit:.2f} (min of {ratio}x reference {ref} and budget {thr['cost']['budget_usd']})")
    assert total <= limit, f"projected {total} > {limit:.2f} USD/month"


@pytest.mark.req("R12")
def test_projection_consistent_with_live(aws, cfg, thr, pricing):
    d, err = load_declared(cfg.get("cost_projection_file"))
    assert d, err
    est, items, unknown = estimate_monthly_cost(aws, pricing)
    if unknown:
        report.warn("R12", f"could not price: {unknown}")
    report.note("R12", f"cost estimated from live resources: {est} {items}")
    assert est <= thr["cost"]["budget_usd"], f"cost estimated from deployed resources ({est}) exceeds the budget"
    tol = thr["cost"]["declared_vs_computed_tolerance"]
    declared = d["_computed_total"]
    assert abs(declared - est) <= tol * max(est, 1.0) + 2.0, \
        f"declared {declared} vs {est} estimated from what is actually deployed (tolerance {tol:.0%})"
