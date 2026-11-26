"""R11-R12: minimal stream and cost projection."""
import pytest

from harness import report
from harness.cost import load_declared


@pytest.mark.req("R11")
def test_stream_is_minimal(aws, A, thr):
    c = thr["cost"]
    s = aws.kinesis_summary(A["kinesis_stream"])
    assert s, f"stream {A['kinesis_stream']} not found"
    report.note("R11", f"stream: {s}")
    if c["require_provisioned"]:
        assert s["mode"] == "PROVISIONED", f"stream mode {s['mode']}: use provisioned mode (on-demand costs more at this volume)"
    assert s["open_shards"] <= c["max_stream_shards"], f"{s['open_shards']} shards (max {c['max_stream_shards']})"
    assert s["retention_hours"] <= c["max_retention_hours"], f"retention {s['retention_hours']} h"
    assert s["consumers"] == 0, f"{s['consumers']} enhanced fan-out consumers (extra cost)"


@pytest.mark.req("R12")
def test_projection_within_budget(cfg, thr):
    d, err = load_declared(cfg.get("cost_projection_file"))
    assert d, err
    assert d["line_items"], "no line_items"
    ref, ratio = thr["cost"]["reference_monthly_usd"], thr["cost"]["max_ratio_vs_reference"]
    limit = min(ref * ratio, thr["cost"]["budget_usd"])
    report.note("R12", f"declared {d['_computed_total']} USD/month; limit {limit:.2f}")
    assert d["_computed_total"] <= limit, f"projected {d['_computed_total']} > {limit:.2f} USD/month"


@pytest.mark.req("R12")
def test_projection_lists_the_services_used(cfg):
    d, err = load_declared(cfg.get("cost_projection_file"))
    assert d, err
    named = " ".join(str(i.get("service", "")).lower() for i in d["line_items"])
    missing = [s for s in ("kinesis", "s3", "athena") if s not in named]
    assert not missing, f"cost projection has no line for: {missing}"
