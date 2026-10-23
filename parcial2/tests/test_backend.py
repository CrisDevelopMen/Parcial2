"""R1-R3: backend behaviour, black-box over HTTP."""
import statistics
import threading
import time
import uuid
import datetime as dt

import httpx
import pytest

from harness import report


@pytest.fixture(scope="module")
def base(cfg):
    return cfg["backend"]["base_url"].rstrip("/")


@pytest.fixture(scope="module")
def route(truth):
    return truth.params["routes"][0]


def _mk_flight(base, route, capacity, date="2030-01-15", price=100000):
    fid = "H" + uuid.uuid4().hex[:10].upper()
    r = httpx.post(f"{base}/admin/flights", timeout=15, json={
        "flight_id": fid, "route": route, "flight_date": date, "capacity": capacity, "price_cents": price})
    assert r.status_code == 201, f"POST /admin/flights returned {r.status_code}: {r.text[:120]}"
    return fid


def _reserve(client, base, fid, name="Harness Tester"):
    return client.post(f"{base}/reservations", json={
        "flight_id": fid, "passenger_name": name, "fare_class": "ECONOMY"})


def _seats_available(base, route, fid, date="2030-01-15"):
    o, d = route.split("-")
    r = httpx.get(f"{base}/flights/search", params={"origin": o, "destination": d, "date": date}, timeout=15)
    assert r.status_code == 200
    for f in r.json():
        if f["flight_id"] == fid:
            return f["seats_available"]
    return 0   # absent from search == no seats


@pytest.mark.req("R1")
def test_search_p95(base, route, thr):
    t = thr["backend"]
    o, d = route.split("-")
    start = dt.date(2030, 3, 1)
    for i in range(10):
        _mk_flight(base, route, 150, str(start + dt.timedelta(days=i)))
    n, conc = t["search_requests"], t["search_concurrency"]
    lat, errs = [], []
    lock = threading.Lock()
    idx = iter(range(n))

    def worker():
        with httpx.Client(timeout=20) as c:
            while True:
                with lock:
                    try:
                        i = next(idx)
                    except StopIteration:
                        return
                day = str(start + dt.timedelta(days=i % 10))
                t0 = time.perf_counter()
                r = c.get(f"{base}/flights/search", params={"origin": o, "destination": d, "date": day})
                dt_ = time.perf_counter() - t0
                with lock:
                    lat.append(dt_)
                    if r.status_code != 200 or not isinstance(r.json(), list) or not r.json():
                        errs.append(r.status_code)

    ths = [threading.Thread(target=worker) for _ in range(conc)]
    [x.start() for x in ths]
    [x.join() for x in ths]
    lat.sort()
    p95 = lat[int(0.95 * len(lat)) - 1]
    report.note("R1", f"{len(lat)} requests, concurrency {conc}: p50={statistics.median(lat):.3f}s p95={p95:.3f}s")
    assert not errs, f"{len(errs)} search responses were not 200 with non-empty list (codes {set(errs)})"
    assert p95 <= t["search_p95_seconds"], f"p95 {p95:.3f}s > {t['search_p95_seconds']}s"


def _race(base, route, capacity, n):
    fid = _mk_flight(base, route, capacity)
    barrier = threading.Barrier(n)
    codes = [None] * n
    clients = [httpx.Client(timeout=30) for _ in range(n)]

    def go(i):
        barrier.wait()
        try:
            codes[i] = _reserve(clients[i], base, fid, f"P{i}").status_code
        except Exception:
            codes[i] = -1

    ths = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [x.start() for x in ths]
    [x.join() for x in ths]
    [c.close() for c in clients]
    return fid, codes


@pytest.mark.req("R2")
def test_last_seat_exactly_one(base, route, thr):
    t = thr["backend"]
    for rnd in range(t["concurrency_rounds"]):
        fid, codes = _race(base, route, 1, t["concurrency_requests"])
        ok, conflict = codes.count(201), codes.count(409)
        report.note("R2", f"round {rnd+1}: 201={ok} 409={conflict} other={len(codes)-ok-conflict}")
        assert ok == 1, f"round {rnd+1}: {ok} reservations created for 1 seat (codes {sorted(set(codes))})"
        assert conflict == len(codes) - 1, f"round {rnd+1}: expected all others 409, got {sorted(set(codes))}"
        assert _seats_available(base, route, fid) == 0


@pytest.mark.req("R2")
def test_k_seats_exactly_k(base, route, truth, thr):
    k = truth.params["small_k"]
    n = thr["backend"]["concurrency_requests"]
    for rnd in range(thr["backend"]["concurrency_rounds"]):
        fid, codes = _race(base, route, k, n)
        ok = codes.count(201)
        report.note("R2", f"K={k} round {rnd+1}: 201={ok}")
        assert ok == k, f"round {rnd+1}: {ok} reservations for {k} seats"
        assert codes.count(409) == n - k, f"unexpected codes {sorted(set(codes))}"


@pytest.mark.req("R3")
def test_get_reservation(base, route):
    fid = _mk_flight(base, route, 5)
    with httpx.Client(timeout=15) as c:
        r = _reserve(c, base, fid, "Ana Gomez")
        assert r.status_code == 201
        body = r.json()
        assert body["status"] == "CONFIRMED" and body["flight_id"] == fid
        g = c.get(f"{base}/reservations/{body['reservation_id']}")
        assert g.status_code == 200
        gb = g.json()
        assert gb["reservation_id"] == body["reservation_id"] and gb["flight_id"] == fid
        assert gb["status"] == "CONFIRMED" and gb["passenger_name"] == "Ana Gomez"


@pytest.mark.req("R3")
def test_unknown_reservation_404(base):
    r = httpx.get(f"{base}/reservations/does-not-exist-{uuid.uuid4().hex}", timeout=15)
    assert r.status_code == 404
