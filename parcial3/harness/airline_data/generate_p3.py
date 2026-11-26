"""python -m airline_data.generate_p3 --seed 417 --out ref/   (writes routes_ref.csv, route_category_history.csv and holidays_ref.csv)"""
import argparse

from .events import write_reference_files

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--out", default="ref")
    a = ap.parse_args()
    write_reference_files(a.seed, a.out)
    print("wrote", a.out)
