"""Small helpers shared by the tests."""
import json
import re
import time

def athena(aws, A, sql):
    return aws.athena_run(sql, A["athena_database"], A.get("athena_workgroup", "primary"))

def poll(fn, timeout, interval=1.0):
    """Calls fn() until it returns a truthy value or timeout; returns (value, elapsed)."""
    t0 = time.time()
    while True:
        v = fn()
        if v:
            return v, time.time() - t0
        if time.time() - t0 >= timeout:
            return v, time.time() - t0
        time.sleep(interval)

def decode_events(records):
    out = []
    for r in records:
        try:
            e = json.loads(r["data"])
            out.append((r["partition_key"], e))
        except Exception:
            pass
    return out

def _scan_ids(text):
    import re
    return re.findall(r'"event_id"\s*:\s*"(rp-[^"]+)"', text)
