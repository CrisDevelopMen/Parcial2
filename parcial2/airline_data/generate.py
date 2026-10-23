"""Synthetic airline history generator.

make_clean()  -> the "truth": clean flights + reservations (never shipped to students as such)
make_dirty()  -> the RAW data students must clean with EMR (duplicates, noisy route codes,
                 noisy status text, invalid prices/status rows). All noise is additive or
                 normalisable, so applying the documented cleaning rules to the raw data
                 reproduces the clean truth EXACTLY (checked in selftest).

CLI:  python -m airline_data.generate --seed 123 --rows 2000000 --out ./raw
"""
import argparse
import os

import numpy as np
import pandas as pd

from .params import params_for_team


def make_clean(seed: int, target_rows: int):
    p = params_for_team(seed)
    rs = np.random.RandomState(seed)
    routes, months, cap = p["routes"], p["months"], p["capacity"]
    R, M = len(routes), len(months)

    route_lf = np.clip(p["mean_load_factor"] + np.linspace(-0.12, 0.12, R)[rs.permutation(R)], 0.4, 0.97)
    route_cancel = np.clip(p["cancel_rate"] + rs.uniform(-0.03, 0.03, R), 0.02, 0.30)
    base_price = rs.randint(8_000, 30_000, R)  # USD cents

    rows_per_flight = cap * p["mean_load_factor"] * (1 + p["cancel_rate"])
    F = max(3, int(round(target_rows / (R * M * rows_per_flight))))
    n_flights = R * M * F

    route_idx = np.repeat(np.arange(R), M * F)
    month_idx = np.tile(np.repeat(np.arange(M), F), R)
    day = rs.randint(1, 29, n_flights)
    month_str = np.array(months)[month_idx]
    flight_date = np.char.add(np.char.add(month_str, "-"), np.char.zfill(day.astype(str), 2))

    season = 0.06 * np.sin(2 * np.pi * (month_idx % 12) / 12)
    lf = np.clip(route_lf[route_idx] + season, 0.3, 0.98)
    sold = rs.binomial(cap, lf)
    cancelled = rs.poisson(sold * route_cancel[route_idx])

    flights = pd.DataFrame({
        "flight_id": np.arange(1, n_flights + 1, dtype=np.int64),
        "route": np.array(routes)[route_idx],
        "flight_date": flight_date,
        "capacity": np.full(n_flights, cap, dtype=np.int64),
    })

    per_flight = sold + cancelled
    total = int(per_flight.sum())
    fid = np.repeat(flights["flight_id"].values, per_flight)
    starts = np.repeat(np.cumsum(per_flight) - per_flight, per_flight)
    offset = np.arange(total) - starts
    is_conf = offset < np.repeat(sold, per_flight)
    status = np.where(is_conf, "CONFIRMED", "CANCELLED")
    biz = rs.rand(total) < 0.15
    res_route_idx = np.repeat(route_idx, per_flight)
    price = (base_price[res_route_idx] * np.where(biz, 2.5, 1.0) * rs.uniform(0.8, 1.3, total)).round().astype(np.int64)

    res = pd.DataFrame({
        "reservation_id": np.arange(1, total + 1, dtype=np.int64),
        "flight_id": fid.astype(np.int64),
        "fare_class": np.where(biz, "business", "economy"),
        "price_cents": price,
        "status": status,
        "days_before_departure": rs.randint(1, 91, total).astype(np.int64),
    })
    return flights, res


def _noisy_route(route: str, variant: int) -> str:
    a, b = route.split("-")
    return [route.lower(), f" {route} ", f"{a} - {b}"][variant]


def make_dirty(flights: pd.DataFrame, res: pd.DataFrame, seed: int):
    rs = np.random.RandomState(seed + 1)
    f = flights.copy()
    n = len(f)
    pick = rs.rand(n) < 0.25
    variant = rs.randint(0, 3, n)
    f.loc[pick, "route"] = [_noisy_route(r, v) for r, v in zip(f.loc[pick, "route"], variant[pick])]

    r = res.copy()
    nr = len(r)
    # status case/whitespace noise (normalisable)
    pick = np.where(rs.rand(nr) < 0.03)[0]
    r.loc[r.index[pick], "status"] = [s.lower() + " " for s in r.loc[r.index[pick], "status"]]

    next_id = int(r["reservation_id"].max()) + 1
    extras = []
    # exact duplicates of existing rows (same reservation_id)
    dup = r.iloc[np.where(rs.rand(nr) < 0.015)[0]]
    extras.append(dup)
    # invalid status rows (new ids)
    k = int(nr * 0.01)
    bad_status = r.iloc[rs.randint(0, nr, k)].copy()
    bad_status["reservation_id"] = np.arange(next_id, next_id + k)
    bad_status["status"] = "PENDING"
    next_id += k
    extras.append(bad_status)
    # invalid price rows (new ids): zero, negative or missing
    k = int(nr * 0.01)
    bad_price = r.iloc[rs.randint(0, nr, k)].copy()
    bad_price["reservation_id"] = np.arange(next_id, next_id + k)
    bad_price["price_cents"] = rs.choice([0, -500, -1], k)
    bad_price["status"] = "CONFIRMED"
    next_id += k
    extras.append(bad_price)

    r = pd.concat([r] + extras, ignore_index=True)
    r["price_cents"] = r["price_cents"].astype("Int64")
    miss = rs.rand(len(r)) < 0.002
    # missing prices only on extra (invalid) rows so truth stays intact
    miss &= (r["reservation_id"] >= int(res["reservation_id"].max()) + 1).values
    r.loc[miss, "price_cents"] = pd.NA
    r = r.sample(frac=1.0, random_state=seed + 2).reset_index(drop=True)
    return f, r


def write_raw(flights_raw, res_raw, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    flights_raw.to_csv(os.path.join(out_dir, "flights.csv"), index=False)
    res_raw.to_csv(os.path.join(out_dir, "reservations.csv"), index=False)


def main():
    ap = argparse.ArgumentParser(description="Generate the RAW airline history for a team seed")
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--out", default="./raw")
    a = ap.parse_args()
    cf, cr = make_clean(a.seed, a.rows)
    fr, rr = make_dirty(cf, cr, a.seed)
    write_raw(fr, rr, a.out)
    print(f"wrote {len(fr)} flights and {len(rr)} raw reservation rows to {a.out}")


if __name__ == "__main__":
    main()
