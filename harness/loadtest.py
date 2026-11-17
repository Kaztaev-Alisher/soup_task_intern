"""Open-loop API load test, standard library only, meant to run INSIDE the Docker
network so that Docker Desktop's Windows port proxy and a slow client are not measured:

    docker compose exec -T -e RPS=100 -e DURATION=60 api python - < harness/loadtest.py

Open loop: requests are *scheduled* at a fixed rate and latency is measured from the
scheduled send time, so a slow server cannot hide its own queueing by slowing the
client down (coordinated omission).

Mix: 40% create, 40% get, 10% list page, 10% cancel.
"""
import http.client
import json
import os
import queue
import random
import threading
import time
import uuid

RPS = float(os.environ.get("RPS", "100"))
DURATION = float(os.environ.get("DURATION", "60"))
THREADS = int(os.environ.get("THREADS", "64"))
HOST, PORT = os.environ.get("TARGET_HOST", "127.0.0.1"), int(os.environ.get("TARGET_PORT", "8000"))

work, lock = queue.Queue(), threading.Lock()
lat = {"create": [], "get": [], "list": [], "cancel": []}
codes, errors, ids, timeline = {}, [0], [], []


def worker():
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    rnd = random.Random()
    while True:
        item = work.get()
        if item is None:
            return
        scheduled, kind = item
        try:
            if kind == "create" or not ids:
                kind = "create"
                body = json.dumps({"payload": {"duration_ms": rnd.randint(20, 150)}})
                conn.request("POST", "/jobs", body=body, headers={
                    "Idempotency-Key": "load-" + uuid.uuid4().hex, "Content-Type": "application/json"})
            elif kind == "get":
                conn.request("GET", "/jobs/" + rnd.choice(ids))
            elif kind == "list":
                conn.request("GET", "/jobs?limit=50&status=queued")
            else:
                conn.request("POST", "/jobs/" + rnd.choice(ids) + "/cancel")
            r = conn.getresponse()
            data = r.read()
            done = time.perf_counter()
            with lock:
                lat[kind].append(done - scheduled)
                timeline.append((scheduled, done - scheduled))
                codes[r.status] = codes.get(r.status, 0) + 1
                if kind == "create" and r.status == 201:
                    ids.append(json.loads(data)["id"])
        except Exception:
            with lock:
                errors[0] += 1
            conn.close()
            conn = http.client.HTTPConnection(HOST, PORT, timeout=30)


def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(p / 100 * len(s)))] * 1000 if s else float("nan")


threads = [threading.Thread(target=worker, daemon=True) for _ in range(THREADS)]
[t.start() for t in threads]
rnd, n, t0 = random.Random(7), int(RPS * DURATION), time.perf_counter()
for i in range(n):
    due = t0 + i / RPS
    delay = due - time.perf_counter()
    if delay > 0:
        time.sleep(delay)
    work.put((due, rnd.choices(["create", "get", "list", "cancel"], [40, 40, 10, 10])[0]))
[work.put(None) for _ in threads]
[t.join() for t in threads]
elapsed = time.perf_counter() - t0

total = sum(len(v) for v in lat.values())
print(f"target {RPS:.0f} rps for {DURATION:.0f}s, {THREADS} client threads, open loop")
print(f"completed {total} of {n} requests in {elapsed:.1f}s = {total / elapsed:.1f} rps; "
      f"transport errors {errors[0]}; HTTP codes {dict(sorted(codes.items()))}")
for kind, v in lat.items():
    print(f"  {kind:6s} n={len(v):5d}  p50={pct(v, 50):7.1f} ms  p95={pct(v, 95):7.1f} ms  "
          f"p99={pct(v, 99):7.1f} ms  max={pct(v, 100):7.1f} ms")
everything = [x for v in lat.values() for x in v]
print(f"  ALL    n={len(everything):5d}  p50={pct(everything, 50):7.1f} ms  p95={pct(everything, 95):7.1f} ms  "
      f"p99={pct(everything, 99):7.1f} ms  max={pct(everything, 100):7.1f} ms")

# Where in time is the tail? One stall and a steady tail look the same in a percentile.
SLOW = float(os.environ.get("SLOW_MS", "100")) / 1000
by_second = {}
for scheduled, latency in timeline:
    by_second.setdefault(int(scheduled - t0), []).append(latency)
slow_seconds = {s: v for s, v in by_second.items() if max(v) > SLOW}
print(f"  seconds (by scheduled send time) containing a request slower than {SLOW * 1000:.0f} ms: "
      f"{len(slow_seconds)} of {len(by_second)}")
for s in sorted(slow_seconds)[:40]:
    v = slow_seconds[s]
    print(f"    t={s:3d}s  requests={len(v):4d}  slow={sum(x > SLOW for x in v):4d}  max={max(v) * 1000:7.1f} ms")
steady = [x for s, v in by_second.items() if s >= 5 for x in v]
print(f"  after the first 5 s only: n={len(steady)}  p50={pct(steady, 50):.1f} ms  p95={pct(steady, 95):.1f} ms  "
      f"p99={pct(steady, 99):.1f} ms  max={pct(steady, 100):.1f} ms")
