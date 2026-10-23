import os
import pytest

from airline_data.oracle import load_truth
from harness import registry, report
from harness.config import load_config, load_thresholds, load_pricing

TITLES = {
    "R1": "Flight search latency", "R2": "No overselling under concurrency", "R3": "Reservation retrieval",
    "R4": "EMR job produces curated Parquet", "R5": "Glue Catalog tables per contract",
    "R6": "Athena correctness and partition pruning", "R7": "Analytics endpoint backed by Athena",
    "R8": "Customer flow in browser", "R9": "Manager panel matches endpoint",
    "R10": "No leftover EMR / bounded EMR hours", "R11": "Simple low-cost architecture",
    "R12": "Cost projection"}

_rec = None


def pytest_configure(config):
    config.addinivalue_line("markers", "req(id): requirement id (R1..R12) this test verifies")
    global _rec
    _rec = report.Recorder(load_thresholds()["weights"], TITLES)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    out = yield
    rep = out.get_result()
    m = item.get_closest_marker("req")
    if not m:
        return
    req = m.args[0]
    if rep.when == "call":
        outcome = {"passed": "passed", "failed": "failed", "skipped": "skipped"}[rep.outcome]
        detail = ""
        if rep.failed and call.excinfo:
            detail = str(call.excinfo.value).splitlines()[0][:300] if str(call.excinfo.value) else call.excinfo.typename
        elif rep.skipped and call.excinfo:
            detail = str(call.excinfo.value)[:300]
        _rec.add(req, item.name, outcome, detail)
    elif rep.when == "setup" and rep.outcome != "passed":
        _rec.add(req, item.name, "failed" if rep.failed else "skipped",
                 (str(call.excinfo.value)[:300] if call.excinfo else ""))


def pytest_sessionfinish(session, exitstatus):
    cfg = None
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
def truth(cfg, thr):
    return load_truth(cfg["seed"], thr["dataset_rows"])


@pytest.fixture(scope="session")
def aws(cfg):
    if registry.AWS_OVERRIDE is not None:
        return registry.AWS_OVERRIDE
    from harness.aws import RealAWS
    a = cfg["aws"]
    return RealAWS(a.get("region", "us-east-1"), a.get("athena_output"))
