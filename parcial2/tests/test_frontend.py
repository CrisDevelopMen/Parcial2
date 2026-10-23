"""R8-R9: real browser (Playwright/Chromium)."""
import uuid

import httpx
import pytest

from harness import report

pw = pytest.importorskip("playwright.sync_api")


@pytest.fixture(scope="module")
def browser():
    with pw.sync_playwright() as p:
        b = p.chromium.launch()
        yield b
        b.close()


@pytest.fixture()
def page(browser):
    ctx = browser.new_context()
    pg = ctx.new_page()
    pg.set_default_timeout(15000)
    yield pg
    ctx.close()


def tid(page, name):
    return page.get_by_test_id(name)


@pytest.mark.req("R8")
def test_search_reserve_confirm(page, cfg, truth):
    be, fe = cfg["backend"]["base_url"].rstrip("/"), cfg["frontend"]["base_url"].rstrip("/")
    route = truth.params["routes"][0]
    o, d = route.split("-")
    fid = "UI" + uuid.uuid4().hex[:8].upper()
    r = httpx.post(f"{be}/admin/flights", timeout=15, json={"flight_id": fid, "route": route,
                   "flight_date": "2031-05-20", "capacity": 3, "price_cents": 120000})
    assert r.status_code == 201
    page.goto(fe + "/")
    tid(page, "search-origin").fill(o)
    tid(page, "search-destination").fill(d)
    tid(page, "search-date").fill("2031-05-20")
    tid(page, "search-submit").click()
    rows = tid(page, "flight-row")
    rows.first.wait_for()
    assert rows.count() >= 1, "no flight rows rendered"
    rows.first.get_by_test_id("reserve-button").click()
    tid(page, "passenger-name").fill("Harness UI")
    tid(page, "confirm-submit").click()
    cid = tid(page, "confirmation-id")
    cid.wait_for()
    rid = cid.inner_text().strip()
    assert rid, "empty confirmation id"
    g = httpx.get(f"{be}/reservations/{rid}", timeout=15)
    assert g.status_code == 200 and g.json()["passenger_name"] == "Harness UI"
    report.note("R8", f"UI created reservation {rid} for a flight; backend returns it")


@pytest.mark.req("R9")
def test_manager_panel_equals_endpoint(page, cfg, truth, thr):
    be, fe = cfg["backend"]["base_url"].rstrip("/"), cfg["frontend"]["base_url"].rstrip("/")
    route, month = truth.probes(cfg["seed"] + 1 + int(thr.get("probe_salt", 0)), 1)[0]
    page.goto(fe + "/manager")
    tid(page, "manager-route").fill(route)
    tid(page, "manager-month").fill(month)
    tid(page, "manager-load").click()
    v = tid(page, "occupancy-value")
    v.wait_for()
    page.wait_for_function("el => /[0-9]/.test(el.textContent)", arg=v.element_handle())
    txt = v.inner_text().strip().replace(",", ".")
    assert "%" in txt, f"occupancy-value should be a percent such as 78.4%, got {txt!r}"
    shown = float(txt.replace("%", "")) / 100
    ep = httpx.get(f"{be}/analytics/occupancy", params={"route": route, "month": month}, timeout=30).json()["occupancy"]
    exp = truth.occupancy(route, month)
    report.note("R9", f"{route} {month}: panel={shown:.4f} endpoint={float(ep):.4f} truth={exp:.4f}")
    assert abs(shown - float(ep)) <= 0.001, f"panel {shown} vs endpoint {ep}"
    assert abs(shown - exp) <= 0.001, f"panel {shown} vs truth {exp}"
