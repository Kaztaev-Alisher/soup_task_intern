#!/usr/bin/env python
"""Build the run table in the README straight from the raw logs in runs/, so that no
number is copied by hand:   python harness/summarize.py

Every chaos log is listed, including smoke runs, negative controls and disturbed runs.
"""
import ast
import re
import sys
from pathlib import Path

RUNS = Path(__file__).resolve().parent.parent / "runs"


def first(pattern, text, default="-"):
    m = re.search(pattern, text)
    return m.group(1) if m else default


def describe(args):
    if args.get("bug"):
        only_kills = args.get("crash_points") == "" and args.get("pause_interval") == 0
        return f"control: {args['bug']}" + (", kill -9 only" if only_kills else "")
    extras = []
    if args.get("rps"):
        extras.append(f"paced {args['rps']:.0f} jobs/s")
    if args.get("db_kill_interval"):
        extras.append("DB kills")
    return "clean" + (", " + ", ".join(extras) if extras else "")


def parse(path):
    text = path.read_text(encoding="utf-8")
    args = ast.literal_eval(first(r"args=(\{.*\})", text, "{}"))
    new = "api_kill_interval" in args                      # harness v2 (16 invariants, fault points)
    result = first(r"RESULT: (PASS|FAIL)", text, "ERROR" if "HARNESS ERROR" in text else "incomplete")
    failed = sorted(set(re.findall(r"\[FAIL\] (I\d+)", text)), key=lambda s: int(s[1:]))
    faults = first(r"fault points fired inside workers: (\{.*\})", text, "")
    return {
        "log": path.name,
        "harness": "v2" if new else "v1",
        "what": describe(args),
        "jobs": args.get("jobs", "-"),
        "kills": first(r"worker kill -9 = (\d+)", text) if new else first(r"chaos: kills=(\d+)", text),
        "freezes": first(r"freezes = (\d+)", text) if new else first(r"chaos: kills=\d+ freezes=(\d+)", text),
        "api": first(r"API kills = (\d+)", text, "0"),
        "db": first(r"DB kills = (\d+)", text, "0"),
        "faults": sum(ast.literal_eval(faults).values()) if faults else 0,
        "rerun": first(r"jobs executed more than once: (\d+)", text),
        "zombie": first(r"stale \(zombie\) writes refused in total: (\d+)", text),
        "wall": first(r"total wall time: ([\d.]+)s", text),
        "checks": first(r"RESULT: PASS \(all (\d+) invariants", text, first(r"RESULT: FAIL \(\d+ of (\d+)", text)),
        "result": result + (" " + ",".join(failed) if failed else ""),
        "clean": not args.get("bug"),
    }


def main():
    rows = [parse(p) for p in sorted(RUNS.glob("2*.log"))]
    cols = [("log", "log file"), ("harness", "harness"), ("what", "what"), ("jobs", "jobs"), ("kills", "worker kill -9"),
            ("freezes", "freezes"), ("api", "API kills"), ("db", "DB kills"), ("faults", "fault points fired"),
            ("rerun", "jobs re-run"), ("zombie", "zombie writes refused"), ("wall", "seconds"), ("result", "result")]
    print("| " + " | ".join(title for _, title in cols) + " |")
    print("|" + "|".join("---" for _ in cols) + "|")
    for r in rows:
        print("| " + " | ".join(str(r[k]) for k, _ in cols) + " |")

    def total(sel, key):
        return sum(int(r[key]) for r in rows if sel(r) and str(r[key]).isdigit())

    print()
    for label, sel in (("clean runs, harness v2 (final)", lambda r: r["clean"] and r["harness"] == "v2"),
                       ("clean runs, harness v1 (superseded)", lambda r: r["clean"] and r["harness"] == "v1"),
                       ("negative controls, all versions", lambda r: not r["clean"])):
        picked = [r for r in rows if sel(r)]
        passed = sum(r["result"] == "PASS" for r in picked)
        recorded = any(str(r["zombie"]).isdigit() for r in picked)   # harness v1 did not record this
        print(f"{label}: {len(picked)} runs, {passed} PASS, {len(picked) - passed} not PASS; "
              f"{total(sel, 'jobs')} jobs, {total(sel, 'kills')} worker kills, {total(sel, 'freezes')} freezes, "
              f"{total(sel, 'api')} API kills, {total(sel, 'db')} DB kills, {total(sel, 'faults')} fault points, "
              f"{total(sel, 'rerun')} jobs re-run, "
              f"{total(sel, 'zombie') if recorded else 'not recorded:'} zombie writes refused")
    return 0


if __name__ == "__main__":
    sys.exit(main())
