"""Collects results per requirement and writes report.json / report.md.

A requirement earns weight * (passed tests / total tests). A skipped or errored-for-lack-of-access
test earns nothing and is listed under "not_verified" - the report never says "passed" for
something that was not actually checked.
"""
import json
import time
from collections import defaultdict

NOTES = defaultdict(list)      # req -> [str]
WARNINGS = defaultdict(list)   # req -> [str]


def note(req, text):
    NOTES[req].append(str(text))


def warn(req, text):
    WARNINGS[req].append(str(text))


class Recorder:
    def __init__(self, weights, titles):
        self.weights, self.titles = weights, titles
        self.results = defaultdict(list)   # req -> [(test, outcome, detail)]

    def add(self, req, test, outcome, detail=""):
        self.results[req].append((test, outcome, detail))

    def build(self, meta):
        reqs = []
        total = got = 0.0
        for r in sorted(self.weights, key=lambda x: int(x[1:])):
            rs = self.results.get(r, [])
            w = self.weights[r]
            passed = sum(1 for _, o, _ in rs if o == "passed")
            if not rs:
                status, pts = "not_verified", 0.0
            else:
                pts = w * passed / len(rs)
                status = "passed" if passed == len(rs) else ("not_verified" if all(o == "skipped" for _, o, _ in rs)
                                                            else "failed")
            total += w
            got += pts
            reqs.append({"id": r, "title": self.titles.get(r, ""), "weight": w, "points": round(pts, 2),
                         "status": status,
                         "tests": [{"name": t, "outcome": o, "detail": d} for t, o, d in rs],
                         "evidence": NOTES.get(r, []), "warnings": WARNINGS.get(r, [])})
        return {"meta": meta, "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "points": round(got, 2), "max_points": total, "requirements": reqs}

    @staticmethod
    def write(rep, base):
        with open(base + ".json", "w", encoding="utf-8") as f:
            json.dump(rep, f, indent=2, ensure_ascii=False)
        lines = [f"# Harness report - {rep['meta'].get('team_id','?')}", "",
                 f"Automated score: **{rep['points']} / {rep['max_points']}**", "",
                 "| Req | Title | Status | Points |", "|---|---|---|---|"]
        for r in rep["requirements"]:
            lines.append(f"| {r['id']} | {r['title']} | {r['status']} | {r['points']}/{r['weight']} |")
        for r in rep["requirements"]:
            lines += ["", f"## {r['id']} {r['title']} - {r['status']}"]
            for t in r["tests"]:
                lines.append(f"- {t['outcome']}: `{t['name']}` {t['detail']}")
            lines += [f"- evidence: {e}" for e in r["evidence"]]
            lines += [f"- WARNING: {w}" for w in r["warnings"]]
        with open(base + ".md", "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
