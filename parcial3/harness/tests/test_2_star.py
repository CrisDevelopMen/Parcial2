"""R5-R10: star schema in Glue/Athena, checked against the oracle built from the seeded replay events."""
import pytest

from airline_data.events import STAR_CONTRACT, FACT_PARTITIONED, FAR_FUTURE, norm_type, calendar_dates, holiday_calendar
from harness import report
from harness.util import athena

LIKE = "LIKE 'rp-%'"


def _close(got, exp, tol):
    return abs(got - exp) <= tol * max(abs(exp), 1)


def run(aws, A, sql):
    return athena(aws, A, sql)["rows"]


def tbl(aws, A, name):
    return aws.glue_get_table(A["athena_database"], name)


def b(x):
    return str(x).lower() == "true"


# ------------------------------------------------------------------------------------------- R5
@pytest.mark.req("R5")
def test_tables_have_required_columns(aws, A, after_refresh):
    problems = []
    for name, req in STAR_CONTRACT.items():
        t = tbl(aws, A, name)
        if not t:
            problems.append(f"{name}: table not found in Glue")
            continue
        cols = {c["name"]: norm_type(c["type"]) for c in t["columns"]}
        cols.update({c["name"]: norm_type(c["type"]) for c in t["partition_keys"]})
        for c, ty in req.items():
            if c not in cols:
                problems.append(f"{name}.{c} missing")
            elif cols[c] != ty:
                problems.append(f"{name}.{c} is {cols[c]}, expected {ty}")
    assert not problems, "; ".join(problems[:6])


@pytest.mark.req("R5")
def test_tables_are_parquet(aws, A, after_refresh):
    bad = []
    for name in STAR_CONTRACT:
        t = tbl(aws, A, name)
        if not t or "parquet" not in (t["serde"] + t["input_format"] + t["classification"]).lower():
            bad.append(name)
    assert not bad, f"not Parquet (or missing): {bad}"


@pytest.mark.req("R5")
def test_fact_search_partitioned_by_month(aws, A, after_refresh):
    t = tbl(aws, A, "fact_search")
    assert t, "fact_search not found"
    assert [p["name"] for p in t["partition_keys"]] == [FACT_PARTITIONED["fact_search"]], f"partition keys {t['partition_keys']}"
    if not t["projection_enabled"]:
        n = aws.glue_partition_count(A["athena_database"], "fact_search")
        assert n >= 3, f"{n} partitions registered in Glue (the replay covers 3 months)"


@pytest.mark.req("R5")
def test_dim_date_is_enriched_correctly(aws, A, after_refresh):
    """weekday name, quarter, holiday flags and long weekends for every day 2023-2027."""
    exp = calendar_dates().set_index("date_key")
    names = set(holiday_calendar().date)
    rows = run(aws, A, "SELECT date_key, CAST(full_date AS VARCHAR), weekday, weekday_name, quarter, is_holiday, "
                       "is_long_weekend, holiday_name FROM dim_date")
    assert len(rows) == len(exp), f"dim_date has {len(rows)} rows, expected {len(exp)} (2023-01-01 to 2027-12-31)"
    bad = {}
    for r in rows:
        k = int(r[0])
        if k not in exp.index:
            bad.setdefault("unexpected date_key", []).append(k)
            continue
        e = exp.loc[k]
        for name, got, want in (("full_date", r[1][:10], e.full_date), ("weekday", int(r[2]), int(e.weekday)),
                                ("weekday_name", r[3], e.weekday_name), ("quarter", int(r[4]), int(e.quarter)),
                                ("is_holiday", b(r[5]), bool(e.is_holiday)),
                                ("is_long_weekend", b(r[6]), bool(e.is_long_weekend))):
            if got != want:
                bad.setdefault(name, []).append((k, got, want))
        if bool(e.is_holiday) and (r[7] in (None, "", "None", "nan")):
            bad.setdefault("holiday_name empty on a holiday", []).append(k)
    assert not bad, "; ".join(f"{n}: {len(v)} wrong, e.g. {v[0]}" for n, v in list(bad.items())[:4])


# ------------------------------------------------------------------------------------------- R6
@pytest.mark.req("R6")
def test_dimension_keys_unique(aws, A, star_ready):
    bad = []
    for t, k in [("dim_date", "date_key"), ("dim_route", "route_key"), ("dim_fare_class", "fare_class_key")]:
        n, d = run(aws, A, f"SELECT COUNT(*), COUNT(DISTINCT {k}) FROM {t}")[0]
        if int(n) == 0 or int(n) != int(d):
            bad.append(f"{t}.{k}: {n} rows, {d} distinct")
    assert not bad, "; ".join(bad)


@pytest.mark.req("R6")
def test_no_orphan_foreign_keys(aws, A, star_ready):
    checks = [("fact_search", "route_key", "dim_route"), ("fact_search", "date_key", "dim_date"),
              ("fact_booking_event", "route_key", "dim_route"), ("fact_booking_event", "date_key", "dim_date"),
              ("fact_booking_event", "fare_class_key", "dim_fare_class")]
    bad = []
    for f, k, d in checks:
        n = int(run(aws, A, f"SELECT COUNT(*) FROM {f} x LEFT JOIN {d} y ON x.{k} = y.{k} WHERE y.{k} IS NULL")[0][0])
        if n:
            bad.append(f"{f}.{k}: {n} orphan rows")
    assert not bad, "; ".join(bad)


@pytest.mark.req("R6")
def test_fact_grain_one_row_per_event(aws, A, star_ready):
    bad = []
    for f in ("fact_search", "fact_booking_event"):
        n, d = run(aws, A, f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {f}")[0]
        if int(n) == 0 or int(n) != int(d):
            bad.append(f"{f}: {n} rows but {d} distinct event_id")
    assert not bad, "; ".join(bad)


# ------------------------------------------------------------------------------------------- R7
@pytest.mark.req("R7")
def test_q1_revenue_by_month_and_fare_class(aws, A, truth, thr, star_ready):
    rows = run(aws, A, f"SELECT d.year, d.month, c.fare_class, SUM(f.revenue_cents) FROM fact_booking_event f "
                       f"JOIN dim_date d ON f.date_key = d.date_key JOIN dim_fare_class c ON f.fare_class_key = c.fare_class_key "
                       f"WHERE f.event_id {LIKE} GROUP BY 1, 2, 3")
    got = {(int(r[0]), int(r[1]), r[2]): int(float(r[3])) for r in rows}
    exp = truth.q1()
    assert set(got) == set(exp), f"groups differ: missing {sorted(set(exp)-set(got))[:3]}, extra {sorted(set(got)-set(exp))[:3]}"
    wrong = [k for k in exp if not _close(got[k], exp[k], thr["star"]["tolerance_rel"])]
    assert not wrong, f"{len(wrong)} groups with wrong revenue, e.g. {wrong[0]}: {got[wrong[0]]} vs {exp[wrong[0]]}"


@pytest.mark.req("R7")
def test_q2_cancellations_per_route(aws, A, truth, star_ready):
    rows = run(aws, A, f"SELECT r.route, SUM(f.cancellations), SUM(f.bookings) FROM fact_booking_event f "
                       f"JOIN dim_route r ON f.route_key = r.route_key WHERE f.event_id {LIKE} GROUP BY 1")
    got = {r[0]: (int(float(r[1])), int(float(r[2]))) for r in rows}
    exp = truth.q2()
    assert got == exp, f"cancellations/bookings per route differ, e.g. {next((k for k in exp if got.get(k) != exp[k]), None)}"


@pytest.mark.req("R7")
def test_q3_bookings_per_weekday_name(aws, A, truth, star_ready):
    rows = run(aws, A, f"SELECT d.weekday_name, SUM(f.bookings) FROM fact_booking_event f "
                       f"JOIN dim_date d ON f.date_key = d.date_key WHERE f.event_id {LIKE} GROUP BY 1")
    got = {r[0]: int(float(r[1])) for r in rows}
    assert got == truth.q3(), f"{got} vs {truth.q3()}"


@pytest.mark.req("R7")
def test_q7_holidays_vs_other_days(aws, A, truth, star_ready):
    s = run(aws, A, f"SELECT d.is_holiday, COUNT(*) FROM fact_search f JOIN dim_date d ON f.date_key = d.date_key "
                    f"WHERE f.event_id {LIKE} GROUP BY 1")
    bk = run(aws, A, f"SELECT d.is_holiday, SUM(f.bookings) FROM fact_booking_event f JOIN dim_date d ON f.date_key = d.date_key "
                     f"WHERE f.event_id {LIKE} GROUP BY 1")
    gs, gb = {b(r[0]): int(float(r[1])) for r in s}, {b(r[0]): int(float(r[1])) for r in bk}
    got = {k: (gs.get(k, 0), gb.get(k, 0)) for k in (False, True)}
    assert got == truth.q7(), f"(searches, bookings) holiday/non-holiday: {got} vs {truth.q7()}"


@pytest.mark.req("R7")
def test_q8_long_weekends(aws, A, truth, star_ready):
    rows = run(aws, A, f"SELECT d.is_long_weekend, SUM(f.bookings) FROM fact_booking_event f "
                       f"JOIN dim_date d ON f.date_key = d.date_key WHERE f.event_id {LIKE} GROUP BY 1")
    got = {b(r[0]): int(float(r[1])) for r in rows}
    exp = truth.q8()
    assert {k: got.get(k, 0) for k in (False, True)} == exp, f"{got} vs {exp}"


@pytest.mark.req("R7")
def test_q4_top10_routes_by_revenue(aws, A, truth, thr, star_ready):
    rows = run(aws, A, f"SELECT r.route, SUM(f.revenue_cents) AS rev FROM fact_booking_event f "
                       f"JOIN dim_route r ON f.route_key = r.route_key WHERE f.event_id {LIKE} "
                       f"GROUP BY 1 ORDER BY rev DESC, r.route LIMIT 10")
    got = [(r[0], int(float(r[1]))) for r in rows]
    exp = truth.q4(10)
    assert [g[0] for g in got] == [e[0] for e in exp], f"routes {[g[0] for g in got]} vs {[e[0] for e in exp]}"
    assert all(_close(g[1], e[1], thr["star"]["tolerance_rel"]) for g, e in zip(got, exp))


@pytest.mark.req("R7")
def test_q6_conversion_inputs_per_route(aws, A, truth, star_ready):
    s = run(aws, A, f"SELECT r.route, COUNT(*) FROM fact_search f JOIN dim_route r ON f.route_key = r.route_key "
                    f"WHERE f.event_id {LIKE} GROUP BY 1")
    bk = run(aws, A, f"SELECT r.route, SUM(f.bookings) FROM fact_booking_event f JOIN dim_route r ON f.route_key = r.route_key "
                     f"WHERE f.event_id {LIKE} GROUP BY 1")
    gs, gb = {r[0]: int(float(r[1])) for r in s}, {r[0]: int(float(r[1])) for r in bk}
    exp = truth.q6()
    got = {r: (gs.get(r, 0), gb.get(r, 0)) for r in exp}
    assert got == exp, f"searches/bookings per route differ, e.g. {next((k for k in exp if got[k] != exp[k]), None)}"
    report.note("R7", "conversion (bookings / searches) per route matches the oracle")


# ------------------------------------------------------------------------------------------- R8
@pytest.mark.req("R8")
def test_refresh_finishes_in_time(refreshed, thr):
    report.note("R8", f"refresh state {refreshed['state']} in {refreshed['seconds']:.0f}s")
    assert refreshed["ok"], f"refresh ended in state {refreshed['state']}"
    assert refreshed["seconds"] <= thr["stream"]["refresh_max_seconds"]


@pytest.mark.req("R8")
def test_duplicates_removed(aws, A, truth, star_ready):
    for f, exp in (("fact_search", truth.n_search_unique), ("fact_booking_event", truth.n_booking_unique)):
        n, d = run(aws, A, f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {f} WHERE event_id {LIKE}")[0]
        assert int(n) == exp and int(d) == exp, f"{f}: {n} rows / {d} distinct replay events, expected {exp} (duplicates must be removed, nothing lost)"


@pytest.mark.req("R8")
def test_late_events_use_event_date(aws, A, truth, star_ready):
    exp = {int(k.replace("-", "")): v for k, v in truth.per_date_counts()["search"].items()}
    rows = run(aws, A, f"SELECT date_key, COUNT(*) FROM fact_search WHERE event_id {LIKE} GROUP BY 1")
    got = {int(r[0]): int(float(r[1])) for r in rows}
    diff = [k for k in set(exp) | set(got) if exp.get(k, 0) != got.get(k, 0)]
    assert not diff, f"{len(diff)} dates with wrong counts, e.g. {diff[0]}: {got.get(diff[0], 0)} vs {exp.get(diff[0], 0)} (date_key must come from event_ts)"


# ------------------------------------------------------------------------------------------- R9
@pytest.mark.req("R9")
def test_route_dimension_is_type2(aws, A, truth, star_ready):
    rows = run(aws, A, "SELECT route, route_category, CAST(valid_from AS VARCHAR), CAST(valid_to AS VARCHAR), is_current "
                       "FROM dim_route ORDER BY route, valid_from")
    by = {}
    for r in rows:
        by.setdefault(r[0], []).append((r[1], r[2][:10], r[3][:10], b(r[4])))
    problems = []
    for route, h in truth.hist.groupby("route"):
        h = h.sort_values("effective_date")
        v = by.get(route, [])
        if len(v) != len(h):
            problems.append(f"{route}: {len(v)} versions, expected {len(h)}")
            continue
        for i, (cat, vf, vt, cur) in enumerate(v):
            hr = h.iloc[i]
            last = i == len(h) - 1
            exp_to = FAR_FUTURE if last else h.iloc[i + 1]["effective_date"]
            if (cat, vf, vt, cur) != (hr["route_category"], hr["effective_date"], exp_to, last):
                problems.append(f"{route} v{i+1}: {(cat, vf, vt, cur)} vs {(hr['route_category'], hr['effective_date'], exp_to, last)}")
                break
    assert not problems, "; ".join(problems[:4])


@pytest.mark.req("R9")
def test_q5_revenue_uses_category_valid_at_event_date(aws, A, truth, thr, star_ready):
    rows = run(aws, A, f"SELECT r.route_category, d.year, d.month, SUM(f.revenue_cents) FROM fact_booking_event f "
                       f"JOIN dim_route r ON f.route_key = r.route_key JOIN dim_date d ON f.date_key = d.date_key "
                       f"WHERE f.event_id {LIKE} GROUP BY 1, 2, 3")
    got = {(r[0], int(r[1]), int(r[2])): int(float(r[3])) for r in rows}
    exp = truth.q5()
    assert set(got) == set(exp), f"groups differ: {sorted(set(exp) ^ set(got))[:3]}"
    wrong = [k for k in exp if not _close(got[k], exp[k], thr["star"]["tolerance_rel"])]
    assert not wrong, f"{len(wrong)} groups differ, e.g. {wrong[0]}: {got[wrong[0]]} vs {exp[wrong[0]]} (as-of join needed)"


# ------------------------------------------------------------------------------------------ R10
@pytest.mark.req("R10")
def test_one_month_query_scans_less(aws, A, truth, thr, star_ready):
    n_jan = int((truth.searches.ym == "2025-01").sum())
    one = athena(aws, A, "SELECT COUNT(*), SUM(results_count) FROM fact_search WHERE month = '2025-01'")
    allq = athena(aws, A, "SELECT COUNT(*), SUM(results_count) FROM fact_search")
    assert int(one["rows"][0][0]) == n_jan, f"month 2025-01 has {one['rows'][0][0]} searches, expected {n_jan}"
    ratio = one["data_scanned_bytes"] / max(allq["data_scanned_bytes"], 1)
    report.note("R10", f"one month scanned {one['data_scanned_bytes']} B vs {allq['data_scanned_bytes']} B for all ({ratio:.2f})")
    assert ratio <= thr["star"]["max_scan_ratio_one_month"], f"scan ratio {ratio:.2f}: the month filter is not pruning partitions"


@pytest.mark.req("R10")
def test_workgroup_has_scan_limit(aws, A, thr):
    wg = aws.athena_workgroup(A.get("athena_workgroup", "primary"))
    cutoff = wg["bytes_cutoff"]
    assert cutoff is not None, "the Athena workgroup has no per-query data usage limit"
    assert cutoff <= thr["star"]["max_workgroup_cutoff_bytes"], f"limit {cutoff} B is above {thr['star']['max_workgroup_cutoff_bytes']} B"
