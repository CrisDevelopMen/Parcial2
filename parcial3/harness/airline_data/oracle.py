"""Cleaning rules + ground truth ("oracle") for the analytics requirements.

CLEANING RULES (these are the rules the students must implement in their EMR job):
  C1  route: remove all whitespace and upper-case it (e.g. ' bog - mde ' -> 'BOG-MDE')
  C2  status: strip whitespace and upper-case it
  C3  keep only status in {CONFIRMED, CANCELLED}
  C4  drop rows with missing price_cents or price_cents <= 0
  C5  drop duplicate reservation_id (keep the first occurrence)
"""
import os

import numpy as np
import pandas as pd

from .generate import make_clean, make_dirty
from .params import params_for_team

METRIC_COLUMNS = ["route", "month", "seats_capacity", "seats_sold", "occupancy",
                  "revenue_cents", "bookings", "cancellations"]
CURATED_COLUMNS = ["reservation_id", "flight_id", "route", "fare_class", "price_cents", "status"]


def clean_flights(raw: pd.DataFrame) -> pd.DataFrame:
    f = raw.copy()
    f["route"] = f["route"].str.replace(r"\s+", "", regex=True).str.upper()
    return f.sort_values("flight_id").reset_index(drop=True)


def clean_reservations(raw: pd.DataFrame) -> pd.DataFrame:
    r = raw.copy()
    r["status"] = r["status"].str.strip().str.upper()
    r = r[r["status"].isin(["CONFIRMED", "CANCELLED"])]
    r = r[r["price_cents"].notna() & (r["price_cents"].astype("float64") > 0)]
    r = r.drop_duplicates(subset="reservation_id", keep="first")
    r["price_cents"] = r["price_cents"].astype("int64")
    return r.sort_values("reservation_id").reset_index(drop=True)


def curated_detail(flights_clean: pd.DataFrame, res_clean: pd.DataFrame) -> pd.DataFrame:
    f = flights_clean.assign(month=flights_clean["flight_date"].str[:7])[["flight_id", "route", "month"]]
    d = res_clean.merge(f, on="flight_id", how="inner")
    return d[CURATED_COLUMNS + ["month"]]


def route_month_metrics(flights_clean: pd.DataFrame, res_clean: pd.DataFrame) -> pd.DataFrame:
    f = flights_clean.assign(month=flights_clean["flight_date"].str[:7])
    d = curated_detail(flights_clean, res_clean)
    key = ["route", "month"]
    cap = f.groupby(key)["capacity"].sum().rename("seats_capacity")
    conf = d[d["status"] == "CONFIRMED"]
    sold = conf.groupby(key).size().rename("seats_sold")
    rev = conf.groupby(key)["price_cents"].sum().rename("revenue_cents")
    book = d.groupby(key).size().rename("bookings")
    canc = d[d["status"] == "CANCELLED"].groupby(key).size().rename("cancellations")
    m = pd.concat([cap, sold, rev, book, canc], axis=1).fillna(0).astype("int64").reset_index()
    m["occupancy"] = m["seats_sold"] / m["seats_capacity"]
    return m[METRIC_COLUMNS].sort_values(["route", "month"]).reset_index(drop=True)


class Truth:
    """Everything the harness needs to know about the correct answers for one team."""

    def __init__(self, seed: int, rows: int):
        self.seed, self.rows = seed, rows
        self.params = params_for_team(seed)
        cf, cr = make_clean(seed, rows)
        self.flights_clean, self.res_clean = cf, cr
        self.flights_raw, self.res_raw = make_dirty(cf, cr, seed)
        self.metrics = route_month_metrics(cf, cr)
        self.detail = curated_detail(cf, cr)
        self.row_count = len(self.detail)
        self.partition_counts = self.detail.groupby("month").size().to_dict()

    def top_routes(self, n: int = 10) -> pd.DataFrame:
        g = self.metrics.groupby("route")[["seats_sold", "seats_capacity"]].sum()
        g["occupancy"] = g["seats_sold"] / g["seats_capacity"]
        return g.sort_values("occupancy", ascending=False).head(n).reset_index()[["route", "occupancy"]]

    def probes(self, probe_seed: int, k: int):
        rs = np.random.RandomState(probe_seed)
        m = self.metrics
        idx = rs.choice(len(m), size=min(k, len(m)), replace=False)
        return [(m.iloc[i]["route"], m.iloc[i]["month"]) for i in idx]

    def occupancy(self, route: str, month: str) -> float:
        row = self.metrics[(self.metrics["route"] == route) & (self.metrics["month"] == month)]
        return float(row["occupancy"].iloc[0])

    def confirmed_revenue(self, month: str) -> int:
        d = self.detail
        return int(d[(d["month"] == month) & (d["status"] == "CONFIRMED")]["price_cents"].sum())


_CACHE = {}


def load_truth(seed: int, rows: int) -> Truth:
    key = (seed, rows)
    if key not in _CACHE:
        _CACHE[key] = Truth(seed, rows)
    return _CACHE[key]
