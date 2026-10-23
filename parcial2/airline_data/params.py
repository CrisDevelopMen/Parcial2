"""Per-team parameters derived deterministically from the team seed.

Everything here (dataset shape, expected answers, concurrency parameters) is a
pure function of the seed, so the generator, the oracle and the harness always
agree. Only numpy.random.RandomState is used because its streams are frozen
across numpy versions (numpy.random.Generator streams are not).
"""
import numpy as np

AIRPORTS = ["BOG", "MDE", "CLO", "CTG", "BAQ", "SMR", "BGA", "PEI", "CUC", "ADZ", "LET", "PSO"]


def month_list(start_year: int = 2023, n_months: int = 24):
    out = []
    y, m = start_year, 1
    for _ in range(n_months):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def params_for_team(seed: int) -> dict:
    rs = np.random.RandomState(seed)
    n_routes = int(rs.randint(8, 15))
    pairs = [(a, b) for a in AIRPORTS for b in AIRPORTS if a != b]
    idx = rs.permutation(len(pairs))[:n_routes]
    routes = [f"{pairs[i][0]}-{pairs[i][1]}" for i in idx]
    return {
        "seed": seed,
        "routes": routes,
        "capacity": int(rs.choice([120, 150, 180])),
        "small_k": int(rs.randint(3, 9)),          # seats for the K>1 concurrency test
        "mean_load_factor": float(rs.uniform(0.70, 0.86)),
        "cancel_rate": float(rs.uniform(0.08, 0.15)),
        "months": month_list(),
    }
