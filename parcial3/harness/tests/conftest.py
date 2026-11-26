import json
import os
import time
import datetime as dt

import httpx
import pytest

from airline_data.events import StarTruth, encode, partition_key
from harness import registry, report
from harness.util import _scan_ids
from harness.config import load_config, load_thresholds, load_pricing

TITLES = {
    "R1": "Every API action emits one valid event", "R2": "No event loss under burst, API stays fast",
    "R3": "Live view fed by the stream (event time)", "R4": "Events reach the S3 landing zone",
    "R5": "Star schema tables per contract", "R6": "Star integrity (keys, orphans, duplicates)",
    "R7": "Business queries return the right numbers", "R8": "Dirty events handled, refresh in time",
    "R9": "Slowly changing dimension (type 2)", "R10": "Partition pruning and scan limit",
    "R11": "Minimal Kinesis stream", "R12": "Cost projection"}

_rec = None


def pytest_configure(config):
    config.addinivalue_line("markers", "req(id): requirement id (R1..R12) this test verifies")
    global _rec
    report.NOTES.clear()
    report.WARNINGS.clear()
    _rec = report.Recorder(load_thresholds()["weights"], TITLES)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    out = yield
    rep = out.get_result()
    m = item.get_closest_marker("req")
    if not m:
        return
    req = m.args[0]
    msg = ""
    if call.excinfo:
        msg = (str(call.excinfo.value).splitlines() or [call.excinfo.typename])[0][:300]
    if rep.when == "call":
        _rec.add(req, item.name, {"passed": "passed", "failed": "failed", "skipped": "skipped"}[rep.outcome], msg)
    elif rep.when == "setup" and rep.outcome != "passed":
        _rec.add(req, item.name, "failed" if rep.failed else "skipped", msg)


def pytest_sessionfinish(session, exitstatus):
    try:
        cfg = load_config()
    except Exception:
        cfg = {}
    rep = _rec.build({"team_id": cfg.get("team_id"), "seed": cfg.get("seed")})
    out = os.environ.get("HARNESS_REPORT", os.path.join(session.config.rootpath, "report"))
    report.Recorder.write(rep, out)
    session.config._harness_report = rep


def pytest_terminal_summary(terminalreporter):
    rep = getattr(terminalreporter.config, "_harness_report", None)
    if rep:
        terminalreporter.write_line(f"HARNESS SCORE: {rep['points']} / {rep['max_points']}")


# ------------------------------------------------------------------------------ fixtures
@pytest.fixture(scope="session")
def cfg():
    return load_config()


@pytest.fixture(scope="session")
def thr():
    return load_thresholds()


@pytest.fixture(scope="session")
def pricing():
    return load_pricing()


@pytest.fixture(scope="session")
def A(cfg):
    return cfg["aws"]


@pytest.fixture(scope="session")
def base(cfg):
    return cfg["backend"]["base_url"].rstrip("/")


@pytest.fixture(scope="session")
def truth(cfg, thr):
    return StarTruth(cfg["seed"], thr["stream"]["replay_events"], int(thr.get("probe_salt", 0)))


@pytest.fixture(scope="session")
def aws(cfg):
    if registry.AWS_OVERRIDE is not None:
        return registry.AWS_OVERRIDE
    from harness.aws import RealAWS
    a = cfg["aws"]
    return RealAWS(a.get("region", "us-east-1"), a.get("athena_output"))


@pytest.fixture(scope="session")
def route0(truth):
    return truth.routes.iloc[0]["route"]








@pytest.fixture(scope="session")
def replayed(aws, A, truth):
    """Injects the seeded dirty event stream into the student's stream, once."""
    t0 = dt.datetime.now(dt.timezone.utc)
    aws.kinesis_put(A["kinesis_stream"], [(partition_key(e), encode(e)) for e in truth.arrivals])
    return t0


@pytest.fixture(scope="session")
def delivered(aws, A, thr, truth, replayed):
    """Waits until every distinct replay event is in the S3 landing zone. Returns dict."""
    ids = {e["event_id"] for e in truth.unique}
    t = thr["stream"]
    since = replayed - dt.timedelta(seconds=30)

    t0 = time.time()
    found = set()
    while True:
        found = set()
        for _, text in aws.s3_read_text_since(A["raw_events_uri"], since):
            for eid in _scan_ids(text):
                if eid in ids:
                    found.add(eid)
        if len(found) == len(ids) or time.time() - t0 >= t["delivery_max_seconds"]:
            break
        time.sleep(t["delivery_poll_seconds"])
    return {"found": len(found), "expected": len(ids), "seconds": time.time() - t0}




@pytest.fixture(scope="session")
def refreshed(base, thr, delivered):
    """Calls POST /admin/refresh-star and waits for SUCCEEDED. Returns dict(ok, state, seconds, error)."""
    t = thr["stream"]
    t0 = time.time()
    try:
        r = httpx.post(f"{base}/admin/refresh-star", timeout=30)
        if r.status_code not in (200, 202):
            return {"ok": False, "state": f"HTTP {r.status_code}", "seconds": 0.0}
    except Exception as ex:
        return {"ok": False, "state": f"error: {ex}", "seconds": 0.0}
    state = "RUNNING"
    while time.time() - t0 < t["refresh_max_seconds"]:
        try:
            state = httpx.get(f"{base}/admin/refresh-status", timeout=30).json().get("state", "?")
        except Exception:
            state = "?"
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(t["refresh_poll_seconds"])
    return {"ok": state == "SUCCEEDED", "state": state, "seconds": time.time() - t0}


@pytest.fixture()
def star_ready(refreshed):
    if not refreshed["ok"]:
        pytest.fail(f"star schema was not built: refresh state {refreshed['state']} after {refreshed['seconds']:.0f}s")


@pytest.fixture()
def after_refresh(refreshed):
    """Tables are usually created by the student's refresh job: look at them only after it ran (pass or fail)."""
    return refreshed
