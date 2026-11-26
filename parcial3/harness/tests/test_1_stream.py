"""R1-R4: events, live view, landing zone. Black box: HTTP API + Kinesis + S3."""
import datetime as dt
import statistics
import threading
import time
import uuid

import httpx
import pytest

from harness.util import decode_events, poll
from harness import report

UTC = dt.timezone.utc
DATE = "2031-03-10"


def mk_flight(base, route, cap=20):
    fid = "H" + uuid.uuid4().hex[:10].upper()
    r = httpx.post(f"{base}/admin/flights", timeout=15, json={"flight_id": fid, "route": route, "flight_date": DATE,
                                                              "capacity": cap, "price_cents": 100000})
    assert r.status_code == 201, f"POST /admin/flights returned {r.status_code}"
    return fid


def search(client, base, route, sid):
    o, d = route.split("-")
    return client.get(f"{base}/flights/search", params={"origin": o, "destination": d, "date": DATE},
                      headers={"X-Session-Id": sid})


def reserve(client, base, fid, name="Harness"):
    r = client.post(f"{base}/reservations", json={"flight_id": fid, "passenger_name": name, "fare_class": "ECONOMY"})
    assert r.status_code == 201, f"POST /reservations returned {r.status_code}"
    return r.json()


def events_for(aws, A, since, wait, want):
    """Reads the stream until want(events) is truthy or wait seconds elapse."""
    def f():
        evs = decode_events(aws.kinesis_read_since(A["kinesis_stream"], since - dt.timedelta(seconds=5), max_seconds=30))
        return evs if want(evs) else None
    v, _ = poll(f, wait, 2.0)
    return v or decode_events(aws.kinesis_read_since(A["kinesis_stream"], since - dt.timedelta(seconds=5), max_seconds=30))


# ------------------------------------------------------------------------------------------- R1
@pytest.mark.req("R1")
def test_each_action_emits_one_valid_event(aws, A, base, route0, thr):
    n = thr["stream"]["emit_samples"]
    since = dt.datetime.now(UTC)
    fid = mk_flight(base, route0)
    sids = [f"h1-{uuid.uuid4().hex}" for _ in range(n)]
    with httpx.Client(timeout=20) as c:
        for s in sids:
            assert search(c, base, route0, s).status_code == 200
        res = [reserve(c, base, fid, f"P{i}") for i in range(3)]
        cx = c.post(f"{base}/reservations/{res[0]['reservation_id']}/cancel")
        assert cx.status_code == 200, f"cancel returned {cx.status_code}"
    rids = {r["reservation_id"] for r in res}
    want = lambda evs: (sum(1 for _, e in evs if e.get("session_id") in set(sids)) >= n
                        and sum(1 for _, e in evs if e.get("reservation_id") in rids) >= 4)
    evs = events_for(aws, A, since, thr["stream"]["emit_wait_seconds"], want)
    mine_s = [(pk, e) for pk, e in evs if e.get("session_id") in set(sids)]
    mine_r = [(pk, e) for pk, e in evs if e.get("reservation_id") in rids]
    report.note("R1", f"{len(mine_s)} search events for {n} calls; {len(mine_r)} reservation events for 3 reservations + 1 cancel")
    assert len(mine_s) == n, f"expected exactly {n} search_performed events, found {len(mine_s)}"
    assert len(mine_r) == 4, f"expected 3 reservation_created + 1 reservation_cancelled, found {len(mine_r)}"
    ids = [e.get("event_id") for _, e in mine_s + mine_r]
    assert len(set(ids)) == len(ids) and all(ids), "event_id missing or repeated"
    now = dt.datetime.now(UTC)
    for pk, e in mine_s + mine_r:
        assert e.get("schema_version") == 1, f"schema_version {e.get('schema_version')!r}"
        ts = dt.datetime.strptime(e["event_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        assert abs((now - ts).total_seconds()) < 300, f"event_ts {e['event_ts']} is not the time it happened"
    for pk, e in mine_s:
        assert e["event_type"] == "search_performed"
        assert pk == e["session_id"], "search partition key must be session_id"
        o, d = route0.split("-")
        assert (e["origin"], e["destination"], e["travel_date"]) == (o, d, DATE)
        assert isinstance(e["results_count"], int)
    types = sorted(e["event_type"] for _, e in mine_r)
    assert types == ["reservation_cancelled", "reservation_created", "reservation_created", "reservation_created"], types
    for pk, e in mine_r:
        assert pk == e["flight_id"] == fid, "reservation partition key must be flight_id"
        assert e["route"] == route0 and e["fare_class"] == "ECONOMY" and isinstance(e["price_cents"], int)
        assert "passenger_name" not in e, "events must not carry personal data"


# ------------------------------------------------------------------------------------------- R2
@pytest.mark.req("R2")
def test_burst_no_loss_and_fast_api(aws, A, base, route0, thr):
    t = thr["stream"]
    n, conc = t["burst_requests"], t["burst_concurrency"]
    since = dt.datetime.now(UTC)
    mk_flight(base, route0)
    sids = [f"h2-{uuid.uuid4().hex}" for _ in range(n)]
    lat, codes, lock = [], [], threading.Lock()
    it = iter(sids)

    def worker():
        with httpx.Client(timeout=30) as c:
            while True:
                with lock:
                    s = next(it, None)
                if s is None:
                    return
                t0 = time.perf_counter()
                r = search(c, base, route0, s)
                el = time.perf_counter() - t0
                with lock:
                    lat.append(el)
                    codes.append(r.status_code)

    ths = [threading.Thread(target=worker) for _ in range(conc)]
    [x.start() for x in ths]
    [x.join() for x in ths]
    lat.sort()
    p95 = lat[int(0.95 * len(lat)) - 1]
    report.note("R2", f"{n} searches, concurrency {conc}: p95={p95:.3f}s, status codes {sorted(set(codes))}")
    assert set(codes) == {200}, f"non-200 responses: {sorted(set(codes))}"
    assert p95 <= t["burst_p95_seconds"], f"p95 {p95:.3f}s > {t['burst_p95_seconds']}s: publishing slows the API"
    sset = set(sids)
    want = lambda evs: sum(1 for _, e in evs if e.get("session_id") in sset) >= n
    evs = events_for(aws, A, since, t["emit_wait_seconds"] * 2, want)
    got = [e for _, e in evs if e.get("session_id") in sset]
    report.note("R2", f"{len(got)} events in the stream for {n} requests")
    assert len(got) == n, f"events lost or duplicated: {len(got)} found for {n} requests"
    assert len({e['session_id'] for e in got}) == n


# ------------------------------------------------------------------------------------------- R3
def live(base):
    r = httpx.get(f"{base}/analytics/live", params={"window": 5}, timeout=15)
    assert r.status_code == 200, f"/analytics/live returned {r.status_code}"
    return r.json()


@pytest.mark.req("R3")
def test_live_reflects_new_events(base, route0, thr):
    fid = mk_flight(base, route0)
    time.sleep(2)
    b0 = live(base)
    with httpx.Client(timeout=20) as c:
        for i in range(10):
            search(c, base, route0, f"h3-{uuid.uuid4().hex}")
        res = [reserve(c, base, fid) for _ in range(3)]
        assert c.post(f"{base}/reservations/{res[0]['reservation_id']}/cancel").status_code == 200

    def done():
        b = live(base)
        return (b["searches"] - b0["searches"] == 10 and b["reservations"] - b0["reservations"] == 3
                and b["cancellations"] - b0["cancellations"] == 1) and b
    b, el = poll(done, thr["stream"]["live_visible_seconds"], 0.5)
    last = live(base)
    report.note("R3", f"live deltas after {el:.1f}s: searches {last['searches']-b0['searches']}/10, "
                      f"reservations {last['reservations']-b0['reservations']}/3, cancellations {last['cancellations']-b0['cancellations']}/1")
    assert b, (f"within {thr['stream']['live_visible_seconds']} s the live view should add 10 searches, 3 reservations, 1 cancellation; "
               f"saw {last['searches']-b0['searches']}, {last['reservations']-b0['reservations']}, {last['cancellations']-b0['cancellations']}")
    exp = last["reservations"] / last["searches"] if last["searches"] else 0.0
    assert abs(float(last["conversion"]) - exp) < 1e-3, f"conversion {last['conversion']} != reservations/searches {exp}"


@pytest.mark.req("R3")
def test_live_uses_event_time(base, truth, replayed, thr):
    """Replay events carry old event_ts: a live view by event time must not count them."""
    time.sleep(thr['stream']['live_settle_seconds'])
    b = live(base)
    n_search = truth.n_search_unique
    report.note("R3", f"live searches after replaying {n_search} old events: {b['searches']}")
    assert b["searches"] < 0.1 * n_search, \
        f"live view counted {b['searches']} searches right after replaying {n_search} OLD events: the window must use event_ts"


# ------------------------------------------------------------------------------------------- R4
@pytest.mark.req("R4")
def test_events_reach_landing_zone(delivered, thr):
    report.note("R4", f"{delivered['found']}/{delivered['expected']} distinct replay events found in S3 after {delivered['seconds']:.0f}s")
    assert delivered["found"] == delivered["expected"], \
        f"only {delivered['found']} of {delivered['expected']} events reached the landing zone within {thr['stream']['delivery_max_seconds']} s"
    assert delivered["seconds"] <= thr["stream"]["delivery_max_seconds"]
