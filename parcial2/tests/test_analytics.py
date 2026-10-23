"""R4-R7: EMR output, Glue Catalog, Athena results and the analytics endpoint."""
import datetime as dt
import time

import httpx
import pytest

from airline_data.oracle import CURATED_COLUMNS, METRIC_COLUMNS
from harness import report


@pytest.fixture(scope="module")
def A(cfg):
    return cfg["aws"]


def athena(aws, A, sql):
    return aws.athena_run(sql, A["athena_database"], A.get("athena_workgroup", "primary"))


def _rel_ok(got, exp, tol):
    return abs(got - exp) <= tol * max(abs(exp), 1e-12)


@pytest.fixture(scope="module")
def detail_tbl(aws, A):
    t = aws.glue_get_table(A["athena_database"], A["tables"]["detail"])
    return t


# ------------------------------------------------------------------------------------- R4
@pytest.mark.req("R4")
def test_emr_run_completed(aws):
    runs = [r for r in aws.compute_runs() if r["state"] == "COMPLETED"]
    report.note("R4", f"{len(runs)} COMPLETED EMR step/job runs found")
    assert runs, "no COMPLETED EMR step or EMR Serverless job run found in the last 45 days"


@pytest.mark.req("R4")
def test_parquet_written_by_emr(aws, detail_tbl, thr):
    assert detail_tbl, "detail table missing in Glue (see R5)"
    files = [f for f in aws.s3_list(detail_tbl["location"]) if f["key"].endswith(".parquet") and f["size"] > 0]
    assert files, f"no .parquet files under {detail_tbl['location']}"
    months = {k for f in files for k in f["key"].split("/") if k.startswith("month=")}
    assert len(months) == thr["analytics"]["expected_partitions"], \
        f"found {len(months)} month=... prefixes, expected {thr['analytics']['expected_partitions']}"
    skew = dt.timedelta(seconds=thr["analytics"]["emr_window_skew_seconds"])
    windows = [(r["started_at"] - skew, (r["ended_at"] or r["started_at"]) + skew)
               for r in aws.compute_runs() if r["state"] == "COMPLETED" and r["started_at"]]
    inside = sum(1 for f in files if any(a <= f["last_modified"] <= b for a, b in windows))
    frac = inside / len(files)
    report.note("R4", f"{inside}/{len(files)} parquet files were written inside a COMPLETED EMR run window")
    assert frac >= thr["analytics"]["emr_window_fraction"], \
        f"only {frac:.0%} of the Parquet files were written during an EMR run (job must produce them, not a laptop upload)"


@pytest.mark.req("R4")
def test_cleaning_row_count_and_revenue(aws, A, truth):
    tbl = A["tables"]["detail"]
    got = int(athena(aws, A, f"SELECT count(*) FROM {tbl}")["rows"][0][0])
    report.note("R4", f"curated rows: expected {truth.row_count}, got {got}")
    assert got == truth.row_count, f"curated row count {got} != expected {truth.row_count} (cleaning rules C1-C5)"
    months = list(truth.partition_counts)
    for m in (months[0], months[len(months) // 2], months[-1]):
        r = athena(aws, A, f"SELECT coalesce(sum(price_cents),0) FROM {tbl} WHERE month='{m}' AND status='CONFIRMED'")
        assert int(float(r["rows"][0][0])) == truth.confirmed_revenue(m), f"confirmed revenue mismatch for {m}"


# ------------------------------------------------------------------------------------- R5
def _check_cols(t, expected, req):
    names = [c["name"] for c in t["columns"]]
    assert names[:len(expected)] == expected, f"columns {names} do not start with {expected}"


@pytest.mark.req("R5")
def test_detail_table_contract(detail_tbl, thr):
    t = detail_tbl
    assert t, "table not found in Glue Data Catalog"
    _check_cols(t, CURATED_COLUMNS, "R5")
    assert [p["name"] for p in t["partition_keys"]] == ["month"], f"partition keys {t['partition_keys']}"
    assert "parquet" in (t["serde"] + t["input_format"] + t["classification"]).lower(), "table is not Parquet"
    if not t["projection_enabled"]:
        pass


@pytest.mark.req("R5")
def test_detail_partitions_registered(aws, A, detail_tbl, thr):
    assert detail_tbl
    if detail_tbl["projection_enabled"]:
        report.note("R5", "partition projection enabled (partitions resolved by Athena, not the catalog)")
        return
    n = aws.glue_partition_count(A["athena_database"], A["tables"]["detail"])
    assert n == thr["analytics"]["expected_partitions"], f"{n} partitions registered, expected {thr['analytics']['expected_partitions']}"


@pytest.mark.req("R5")
def test_metrics_table_contract(aws, A):
    t = aws.glue_get_table(A["athena_database"], A["tables"]["metrics"])
    assert t, "metrics table not found in Glue Data Catalog"
    _check_cols(t, METRIC_COLUMNS, "R5")
    assert "parquet" in (t["serde"] + t["input_format"] + t["classification"]).lower(), "metrics table is not Parquet"


# ------------------------------------------------------------------------------------- R6
@pytest.mark.req("R6")
def test_top10_routes(aws, A, truth, thr):
    tbl = A["tables"]["metrics"]
    r = athena(aws, A, f"SELECT route, CAST(sum(seats_sold) AS DOUBLE)/sum(seats_capacity) AS occ "
                       f"FROM {tbl} GROUP BY route ORDER BY occ DESC LIMIT 10")
    exp = truth.top_routes(10)
    got = [(row[0], float(row[1])) for row in r["rows"]]
    assert [g[0] for g in got] == list(exp["route"]), f"top-10 routes differ: {[g[0] for g in got]} vs {list(exp['route'])}"
    tol = thr["analytics"]["tolerance_rel"]
    for (route, occ), e in zip(got, exp["occupancy"]):
        assert _rel_ok(occ, float(e), tol), f"{route}: {occ} vs {float(e)}"
    report.note("R6", "top-10 routes and occupancies match the oracle within tolerance")


@pytest.mark.req("R6")
def test_partition_pruning_scan(aws, A, truth, thr, detail_tbl):
    assert detail_tbl
    tbl = A["tables"]["detail"]
    files = [f for f in aws.s3_list(detail_tbl["location"]) if f["key"].endswith(".parquet")]
    assert files
    avg_part = sum(f["size"] for f in files) / thr["analytics"]["expected_partitions"]
    m = list(truth.partition_counts)[12]
    r = athena(aws, A, f"SELECT count(*), sum(price_cents) FROM {tbl} WHERE month='{m}'")
    assert int(r["rows"][0][0]) == truth.partition_counts[m]
    scanned = r["data_scanned_bytes"]
    limit = thr["analytics"]["max_scan_vs_one_partition"] * avg_part
    report.note("R6", f"one-month query scanned {scanned} B; limit {limit:.0f} B (avg partition {avg_part:.0f} B)")
    assert scanned <= limit, f"scanned {scanned} B > {limit:.0f} B: partitions/columnar format are not being exploited"


# ------------------------------------------------------------------------------------- R7
@pytest.mark.req("R7")
def test_endpoint_matches_oracle_and_uses_athena(aws, A, cfg, truth, thr):
    base = cfg["backend"]["base_url"].rstrip("/")
    a = thr["analytics"]
    probes = truth.probes(cfg["seed"] + int(thr.get("probe_salt", 0)), a["endpoint_probes"])
    t_start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)
    for route, month in probes:
        t0 = time.perf_counter()
        r = httpx.get(f"{base}/analytics/occupancy", params={"route": route, "month": month}, timeout=a["endpoint_max_seconds"] + 5)
        el = time.perf_counter() - t0
        assert r.status_code == 200, f"{route} {month}: HTTP {r.status_code}"
        b = r.json()
        assert el <= a["endpoint_max_seconds"], f"{route} {month} took {el:.1f}s"
        assert _rel_ok(float(b["occupancy"]), truth.occupancy(route, month), a["tolerance_rel"]), \
            f"{route} {month}: {b['occupancy']} vs {truth.occupancy(route, month)}"
        row = truth.metrics[(truth.metrics.route == route) & (truth.metrics.month == month)].iloc[0]
        assert int(b["seats_sold"]) == int(row["seats_sold"]) and int(b["seats_capacity"]) == int(row["seats_capacity"])
        report.note("R7", f"{route} {month}: ok in {el:.2f}s")
    execs = [e for e in aws.athena_list_executions(A.get("athena_workgroup", "primary"), 50)
             if e["submitted_at"] and e["submitted_at"] >= t_start and e["state"] == "SUCCEEDED"]
    report.note("R7", f"{len(execs)} Athena queries submitted during the probes")
    assert execs, "no Athena query was executed while serving the endpoint (data must come from Athena)"


@pytest.mark.req("R7")
def test_endpoint_unknown_is_404(cfg):
    base = cfg["backend"]["base_url"].rstrip("/")
    r = httpx.get(f"{base}/analytics/occupancy", params={"route": "ZZZ-YYY", "month": "2023-01"}, timeout=30)
    assert r.status_code == 404
