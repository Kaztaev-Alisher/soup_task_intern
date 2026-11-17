#!/usr/bin/env python
"""Chaos harness for the job queue.

Submits N jobs through the HTTP API while, in parallel:
  * kill      a random worker container gets SIGKILL (docker kill -s KILL) every ~2 s and
              is started again 0.2-1.0 s later;
  * freeze    a random worker is frozen (docker pause) for 1.4-2.2 leases and resumed: a
              "zombie" that still believes it owns its job;
  * fault points inside the worker (CRASH_POINTS / FREEZE_POINTS): the worker SIGKILLs or
              freezes itself at exact places - right after a claim, inside the completion
              transaction, right after its COMMIT. Random kills almost never land there;
  * API kill  the API container gets SIGKILL every ~20 s (beyond what the task asks; it
              makes "the client retries a POST whose answer was lost" happen for real);
  * DB kill   optional (--db-kill-interval), off by default;
  * identical POSTs are repeated (concurrently and later), some with a changed body, and
    ~1% of creates give up after 1 ms and retry blind;
  * jobs are cancelled, at random and deliberately while a live worker is running them.

Then it stops the chaos, lets zombies wake up, and checks invariants straight in the
database. Everything is printed and also written to runs/<timestamp>.log.
Exit code: 0 = all invariants hold, 1 = violation, 2 = harness error.

--bug BUG_NO_FENCING | BUG_RACY_CLAIM | BUG_SPLIT_TX starts the stack with one protection
removed. Those runs are EXPECTED TO FAIL: they show the harness can see the bug.
"""
import argparse
import asyncio
import datetime as dt
import os
import platform
import random
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
# 127.0.0.1, not localhost: on Windows "localhost" tries IPv6 first (measured 49 ms vs 7.5 ms per request)
API = os.environ.get("API_URL", "http://127.0.0.1:8000")
DSN = os.environ.get("HARNESS_DSN", "postgresql://queue:queue@127.0.0.1:55433/queue")
BUGS = ("BUG_NO_FENCING", "BUG_RACY_CLAIM", "BUG_SPLIT_TX")
DEFAULT_CRASH_POINTS = "claimed:0.003,mid_complete:0.003,after_complete:0.003"
DEFAULT_FREEZE_POINTS = "mid_complete:0.002"


class Out:
    def __init__(self, path):
        self.f = open(path, "w", encoding="utf-8")
        self.lock = threading.Lock()

    def __call__(self, msg=""):
        line = f"[{dt.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}"
        with self.lock:
            print(line, flush=True)
            self.f.write(line + "\n")
            self.f.flush()


def docker(*args, env=None):
    try:
        return subprocess.run(["docker", *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(args, 124, "", "docker command timed out")


def wait_api(timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(API + "/healthz", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise RuntimeError("API did not become healthy")


class Db:
    """The harness's own view of the database. Reconnects, because the database itself
    may be one of the things being killed."""

    def __init__(self):
        self.conn, self.lock = None, threading.Lock()

    def q(self, sql, params=None):
        with self.lock:
            for _ in range(90):
                try:
                    if self.conn is None or self.conn.closed:
                        self.conn = psycopg.connect(DSN, autocommit=True, row_factory=dict_row, connect_timeout=5)
                    cur = self.conn.execute(sql, params)
                    return cur.fetchall() if cur.description else []
                except psycopg.OperationalError:
                    try:
                        if self.conn is not None:
                            self.conn.close()
                    except psycopg.Error:
                        pass
                    self.conn = None
                    time.sleep(1)
            raise RuntimeError("database unreachable for 90 s")

    def one(self, sql, params=None):
        return list(self.q(sql, params)[0].values())[0]

    def status_counts(self):
        return {r["status"]: r["n"] for r in self.q("SELECT status, count(*) n FROM jobs GROUP BY 1")}


# ------------------------------------------------------------------ chaos

class Chaos:
    """External fault injector. Each loop ticks start-to-start, so slow docker commands
    and the restart/thaw that follows a fault do not stretch the interval."""

    def __init__(self, containers, out, args):
        self.workers, self.api, self.db = containers["worker"], containers["api"], containers["db"]
        self.out, self.args = out, args
        self.busy, self.lock, self.stop = set(), threading.Lock(), threading.Event()
        self.kills = self.kill_misses = self.pauses = self.api_kills = self.db_kills = self.errors = 0
        self.followups = []
        loops = ((self._killer, args.kill_interval), (self._freezer, args.pause_interval),
                 (self._api_killer, args.api_kill_interval), (self._db_killer, args.db_kill_interval))
        self.threads = [threading.Thread(target=fn, args=(random.Random(args.seed + i + 1), interval), daemon=True)
                        for i, (fn, interval) in enumerate(loops) if interval > 0]

    def _ticks(self, interval, rnd):
        next_at = time.monotonic()
        while True:
            next_at = max(next_at + interval * rnd.uniform(0.5, 1.5), time.monotonic())
            if self.stop.wait(next_at - time.monotonic()):
                return
            yield

    def _pick(self, rnd):
        with self.lock:
            free = [c for c in self.workers if c not in self.busy]
            if not free:
                return None
            c = rnd.choice(free)
            self.busy.add(c)
            return c

    def _release(self, c):
        with self.lock:
            self.busy.discard(c)

    def _later(self, fn, *a):
        t = threading.Thread(target=fn, args=a, daemon=True)
        t.start()
        self.followups.append(t)

    def _killer(self, rnd, interval):
        for _ in self._ticks(interval, rnd):
            c = self._pick(rnd)
            if c is None:
                continue
            r = docker("kill", "-s", "KILL", c)
            if r.returncode == 0:
                self.kills += 1
                self.out(f"CHAOS kill -9  worker {c[:12]}")
                self._later(self._restart, c, rnd.uniform(0.2, 1.0))
            else:
                # Either it was already down (it had just killed itself at a crash point and
                # Docker is restarting it), or docker reported a failure for a kill that did
                # happen ("did not receive an exit event"). Make sure it comes back either way:
                # in runs/20261116-170642-dbkill.log it did not, and that run finished its last
                # two minutes with 7 workers instead of 8.
                self.kill_misses += 1
                self.out(f"CHAOS kill missed worker {c[:12]}: {r.stderr.strip()[-90:]}")
                self._later(self._restart, c, 0.5, False)

    def _restart(self, c, delay, report=True):
        try:
            time.sleep(delay)
            r = docker("start", c)
            if r.returncode != 0 and report:
                self.errors += 1
                self.out(f"CHAOS start FAILED worker {c[:12]}: {r.stderr.strip()[-90:]}")
        finally:
            self._release(c)

    def _freezer(self, rnd, interval):
        for _ in self._ticks(interval, rnd):
            c = self._pick(rnd)
            if c is None:
                continue
            if docker("pause", c).returncode != 0:
                self._release(c)
                continue
            hold = self.args.lease * rnd.uniform(1.4, 2.2)
            self.pauses += 1
            self.out(f"CHAOS freeze   worker {c[:12]} for {hold:.1f}s (lease {self.args.lease}s) -> zombie")
            self._later(self._thaw, c, hold)

    def _thaw(self, c, hold):
        try:
            self.stop.wait(hold)
            docker("unpause", c)
        finally:
            self._release(c)

    def _api_killer(self, rnd, interval):
        for _ in self._ticks(interval, rnd):
            if docker("kill", "-s", "KILL", self.api).returncode == 0:
                self.api_kills += 1
                self.out(f"CHAOS kill -9  API    {self.api[:12]}")
            time.sleep(rnd.uniform(0.2, 1.0))
            docker("start", self.api)

    def _db_killer(self, rnd, interval):
        for _ in self._ticks(interval, rnd):
            if docker("kill", "-s", "KILL", self.db).returncode == 0:
                self.db_kills += 1
                self.out(f"CHAOS kill -9  DATABASE {self.db[:12]}")
            time.sleep(rnd.uniform(0.2, 1.0))
            docker("start", self.db)

    def start(self):
        for t in self.threads:
            t.start()

    def stop_and_heal(self):
        self.stop.set()
        for t in self.threads + self.followups:
            t.join(timeout=120)
        for c in [self.db, self.api, *self.workers]:
            docker("unpause", c)
            docker("start", c)


# ------------------------------------------------------------------- load

class Stats:
    def __init__(self):
        self.lat = []
        self.http_5xx = self.transport_errors = self.forced_timeouts = 0
        self.created = self.replayed = self.conflicts = self.gets = 0
        self.ids = {}                       # idempotency key -> job id
        self.cancels = defaultdict(list)    # job id -> [(http code, status in response)]
        self.violations = []
        self.submitted = 0


class ThreadedClient:
    """One shared sync httpx.Client driven from a thread pool.

    The first version of this harness used httpx.AsyncClient. On Windows that client
    topped out at ~40 req/s with multi-second tails against an API that serves ~330
    req/s to plain threads, so the harness was measuring itself. See README, "what
    surprised me". Latency is timed inside the worker thread, around the HTTP call only.
    """

    def __init__(self, concurrency):
        n = concurrency * 2
        self.http = httpx.Client(base_url=API, transport=httpx.HTTPTransport(
            limits=httpx.Limits(max_connections=n, max_keepalive_connections=n)))
        self.pool = ThreadPoolExecutor(n)

    def _call(self, method, url, kw):
        t = time.perf_counter()
        r = self.http.request(method, url, **kw)
        r.read()
        return r, time.perf_counter() - t

    async def request(self, method, url, **kw):
        return await asyncio.get_running_loop().run_in_executor(self.pool, self._call, method, url, kw)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.pool.shutdown(wait=False, cancel_futures=True)
        self.http.close()


async def request(client, st, method, url, first_timeout=None, **kw):
    """Retry on transport errors and 5xx (incl. 503 + Retry-After), like a real client would."""
    for attempt in range(12):
        try:
            timeout = first_timeout if (attempt == 0 and first_timeout) else 10
            r, elapsed = await client.request(method, url, timeout=timeout, **kw)
            if r.status_code < 500:
                st.lat.append(elapsed)
                return r
            st.http_5xx += 1
        except httpx.HTTPError:
            if attempt == 0 and first_timeout:
                st.forced_timeouts += 1
            else:
                st.transport_errors += 1
        await asyncio.sleep(min(0.05 * 2 ** attempt, 2))
    raise RuntimeError(f"{method} {url}: no answer after 12 attempts")


async def post_job(client, st, key, payload, rnd):
    # ~1% of creates: the request is sent in full, then the client gives up after 1 ms of
    # waiting for the answer. The server still processes it; the client retries blind.
    first_timeout = httpx.Timeout(10, read=0.001) if rnd.random() < 0.01 else None
    return await request(client, st, "POST", "/jobs", first_timeout=first_timeout,
                         json={"payload": payload}, headers={"Idempotency-Key": key})


def make_payload(rnd, i):
    long_job = rnd.random() < 0.01     # longer than the lease: must survive through heartbeats
    return {"n": i,
            "duration_ms": rnd.randint(2000, 7000) if long_job else rnd.randint(20, 150),
            "fail": rnd.random() < 0.02}


async def run_load(args, out, st, db, chaos, keys, payloads):
    rnd = random.Random(args.seed)
    drained = asyncio.Event()
    side = []
    sem = asyncio.Semaphore(args.concurrency)
    deadline = time.monotonic() + args.timeout

    async with ThreadedClient(args.concurrency) as client:

        async def cancel(job_id):
            r = await request(client, st, "POST", f"/jobs/{job_id}/cancel")
            st.cancels[job_id].append((r.status_code, r.json().get("status")))

        async def cancel_later(key):
            await asyncio.sleep(rnd.uniform(0, 15))
            await cancel(st.ids[key])

        async def replay(key):
            await asyncio.sleep(rnd.uniform(0, 5))
            if rnd.random() < 0.1:   # same key, different body -> must be refused
                r = await request(client, st, "POST", "/jobs", json={"payload": {"tampered": True}},
                                  headers={"Idempotency-Key": key})
                if r.status_code == 409:
                    st.conflicts += 1
                else:
                    st.violations.append(f"key {key}: different body accepted with HTTP {r.status_code}")
                return
            r = await post_job(client, st, key, payloads[key], rnd)
            if r.status_code != 200 or r.json()["id"] != st.ids[key]:
                st.violations.append(f"key {key}: replay gave HTTP {r.status_code} {r.text[:120]}")
            else:
                st.replayed += 1

        async def get_later(key):
            await asyncio.sleep(rnd.uniform(0, 10))
            r = await request(client, st, "GET", f"/jobs/{st.ids[key]}")
            if r.status_code != 200:
                st.violations.append(f"key {key}: GET gave HTTP {r.status_code} for an acknowledged job")
            st.gets += 1

        async def submit(i, key):
            if args.rps:
                await asyncio.sleep(max(0.0, t0 + i / args.rps - time.perf_counter()))
            async with sem:
                n = 3 if rnd.random() < 0.15 else 1   # burst of identical concurrent POSTs
                res = await asyncio.gather(*[post_job(client, st, key, payloads[key], rnd) for _ in range(n)])
            bad = [r for r in res if r.status_code not in (200, 201)]
            if bad:
                st.violations.append(f"key {key}: POST gave HTTP {bad[0].status_code} {bad[0].text[:120]}")
                return
            ids = {r.json()["id"] for r in res}
            n201 = sum(r.status_code == 201 for r in res)
            if len(ids) != 1:
                st.violations.append(f"key {key}: identical POSTs returned different job ids {sorted(ids)}")
            if n201 > 1:
                st.violations.append(f"key {key}: {n201} responses claimed 201 Created")
            st.created += n201
            st.replayed += len(res) - n201
            st.ids[key] = res[0].json()["id"]
            st.submitted += 1
            if rnd.random() < 0.15:
                side.append(asyncio.create_task(replay(key)))
            if rnd.random() < args.cancel_frac:
                side.append(asyncio.create_task(cancel_later(key)))
            if rnd.random() < 0.3:
                side.append(asyncio.create_task(get_later(key)))

        async def cancel_running():
            """Aim cancels at jobs a LIVE worker is executing right now (claimed or
            heartbeated within the last 400 ms): the cancel/complete race.
            Orphans of killed workers are deliberately not targeted. The first version
            picked any 'running' job; orphans sit in that state for a whole lease, so it
            cancelled ~90% of them before they could be requeued and silently removed
            the recovery path from the test (see runs/20261116-161953.log: 40 kills, 0 re-runs)."""
            while not drained.is_set():
                rows = await asyncio.to_thread(
                    db.q, "SELECT id::text AS id FROM jobs WHERE status = 'running' "
                          "AND updated_at > now() - interval '400 milliseconds' ORDER BY random() LIMIT 2")
                await asyncio.gather(*[cancel(r["id"]) for r in rows])
                await asyncio.sleep(args.cancel_running_interval)

        async def monitor():
            while not drained.is_set():
                await asyncio.sleep(5)
                c = await asyncio.to_thread(db.status_counts)
                out(f"progress submitted={st.submitted}/{len(keys)} db={dict(sorted(c.items()))} "
                    f"kills={chaos.kills} freezes={chaos.pauses}")

        t0 = time.perf_counter()
        bg = [asyncio.create_task(monitor())]
        if args.cancel_running_interval > 0:
            bg.append(asyncio.create_task(cancel_running()))
        await asyncio.gather(*[submit(i, k) for i, k in enumerate(keys)])
        submit_s = time.perf_counter() - t0
        out(f"all {len(keys)} keys submitted in {submit_s:.1f}s; waiting for replays/cancels and for the queue to drain")
        await asyncio.gather(*side)
        timed_out = False
        while True:
            c = await asyncio.to_thread(db.status_counts)
            if c.get("queued", 0) + c.get("running", 0) == 0:
                break
            if time.monotonic() > deadline:
                timed_out = True
                out(f"TIMEOUT after {args.timeout}s with {c}")
                break
            await asyncio.sleep(1)
        drained.set()
        await asyncio.gather(*bg)
    return submit_s, timed_out


# ------------------------------------------------------------- invariants

def check_invariants(out, db, st, keys):
    results = []

    def check(name, bad, detail=""):
        results.append((name, bad == 0))
        out(f"  [{'PASS' if bad == 0 else 'FAIL'}] {name}" + (f"  -> {bad} violation(s) {detail}" if bad else ""))

    jobs = db.q("SELECT id::text AS id, idempotency_key, status, attempts, requeues, payload, error FROM jobs")
    by_key = {j["idempotency_key"]: j for j in jobs}
    by_id = {j["id"]: j for j in jobs}

    missing = [k for k in keys if k not in by_key]
    wrong_id = [k for k in keys if k in by_key and st.ids.get(k) != by_key[k]["id"]]
    check("I1  no job lost: every acknowledged key is in the DB with the id the client got",
          len(missing) + len(wrong_id), f"missing={missing[:5]} wrong_id={wrong_id[:5]}")
    check("I2  no duplicate job: exactly one row per idempotency key", abs(len(jobs) - len(keys)),
          f"rows={len(jobs)} keys={len(keys)}")

    stuck = [j for j in jobs if j["status"] in ("queued", "running")]
    check("I3  nothing left queued/running after chaos stopped", len(stuck),
          str([(j["id"], j["status"], j["attempts"]) for j in stuck[:5]]))

    dup = db.q("SELECT job_id::text AS job_id, count(*) n FROM side_effects GROUP BY 1 HAVING count(*) > 1")
    check("I4  no job has more than one side effect", len(dup), str([(d["job_id"], d["n"]) for d in dup[:5]]))
    check("I5  every succeeded job has exactly one side effect", db.one(
        "SELECT count(*) FROM jobs j WHERE status = 'succeeded' "
        "AND (SELECT count(*) FROM side_effects s WHERE s.job_id = j.id) <> 1"))
    check("I6  no failed/cancelled job has a side effect", db.one(
        "SELECT count(*) FROM side_effects s LEFT JOIN jobs j ON j.id = s.job_id "
        "WHERE j.id IS NULL OR j.status <> 'succeeded'"))
    check("I7  the side effect was written by the last (winning) attempt", db.one(
        "SELECT count(*) FROM side_effects s JOIN jobs j ON j.id = s.job_id WHERE s.attempt <> j.attempts"))
    check("I8  no double claim: attempts = requeues + 1, and one execution row per attempt", db.one(
        "SELECT count(*) FROM jobs j WHERE (attempts > 0 AND attempts <> requeues + 1) "
        "OR (attempts = 0 AND requeues <> 0) "
        "OR attempts <> (SELECT count(*) FROM executions e WHERE e.job_id = j.id)"))
    check("I9  terminal rows are clean: finished_at set, no lease left behind", db.one(
        "SELECT count(*) FROM jobs WHERE status IN ('succeeded','failed','cancelled') "
        "AND (finished_at IS NULL OR lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL)"))

    bad_cancel = []
    for job_id, answers in st.cancels.items():
        final = by_id.get(job_id, {}).get("status")
        for code, reported in answers:
            if code == 200 and final != "cancelled":
                bad_cancel.append((job_id, "cancel acknowledged with 200", "final=" + str(final)))
            elif code == 409 and (reported not in ("succeeded", "failed") or final != reported):
                bad_cancel.append((job_id, f"409 reported {reported}", "final=" + str(final)))
            elif code not in (200, 409):
                bad_cancel.append((job_id, f"HTTP {code}", ""))
    check("I10 cancel is honest: 200 => job ends cancelled; 409 => job ended as reported",
          len(bad_cancel), str(bad_cancel[:5]))

    wrong_outcome = [j["id"] for j in jobs if
                     (j["status"] == "succeeded" and j["payload"].get("fail")) or
                     (j["status"] == "failed" and not j["payload"].get("fail")
                      and not (j["error"] or "").startswith("max attempts"))]
    check("I11 outcome matches the job: 'fail' jobs never succeed, others fail only via max attempts",
          len(wrong_outcome), str(wrong_outcome[:5]))
    check("I12 idempotent create held on the client side (same id, one 201, 409 on changed body)",
          len(st.violations), str(st.violations[:5]))

    seen, cursor, pages = [], 0, 0
    with httpx.Client(base_url=API, timeout=30) as c:
        while cursor is not None:
            page = c.get("/jobs", params={"limit": 500, "cursor": cursor}).json()
            seen += [j["id"] for j in page["items"]]
            cursor, pages = page["next_cursor"], pages + 1
    check("I13 list pagination returns every job exactly once",
          int(len(seen) != len(set(seen))) + len(set(seen) ^ set(by_id)), f"pages={pages} seen={len(seen)}")

    # Per-window checks, from the fault log written by the workers' own crash/freeze points.
    check("I14 an attempt killed before its COMMIT (after claim / inside the completion tx) left no side effect",
          db.one("SELECT count(*) FROM fault_log f JOIN side_effects s ON s.job_id = f.job_id "
                 "AND s.attempt = f.attempt WHERE f.point IN ('claimed', 'mid_complete')"))
    check("I15 an attempt killed right after its COMMIT is final: succeeded, one effect, never re-run",
          db.one("SELECT count(*) FROM fault_log f JOIN jobs j ON j.id = f.job_id "
                 "WHERE f.point = 'after_complete' AND (j.status <> 'succeeded' OR j.attempts <> f.attempt "
                 "OR (SELECT count(*) FROM side_effects s WHERE s.job_id = f.job_id AND s.attempt = f.attempt) <> 1)"))
    check("I16 an attempt frozen inside its completion tx past the idle-in-transaction timeout never commits",
          db.one("SELECT count(*) FROM fault_log f JOIN side_effects s ON s.job_id = f.job_id "
                 "AND s.attempt = f.attempt WHERE f.point = 'mid_complete_freeze'"))
    return results


def pct(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, int(p / 100 * len(s)))]


REJECTED_BY_REASON = """
    SELECT kind, count(*) AS n,
           CASE WHEN seen_attempts > attempt THEN 'ZOMBIE: a newer attempt already owned the job'
                WHEN seen_status = 'cancelled' THEN 'job had been cancelled'
                WHEN seen_status = 'queued' THEN 'ZOMBIE: lease expired, job was back in the queue'
                ELSE 'job was ' || coalesce(seen_status, '?') END AS reason
      FROM rejected_writes GROUP BY 1, 3 ORDER BY 3, 1"""


def report_stats(out, db, st, chaos, args, submit_s, chaos_s, total_s):
    out("--- what the run actually exercised (a green run that exercised nothing proves nothing) ---")
    out(f"  final states: {dict(sorted(db.status_counts().items()))}")
    attempts = {r["attempts"]: r["n"] for r in db.q("SELECT attempts, count(*) n FROM jobs GROUP BY 1 ORDER BY 1")}
    out(f"  executions per job (attempts -> jobs): {attempts}")
    rerun = sum(v for k, v in attempts.items() if k > 1)
    out(f"  jobs executed more than once: {rerun}   side_effects rows: {db.one('SELECT count(*) FROM side_effects')}")
    out("  writes refused by fencing, by what the job looked like at that moment:")
    zombie_writes = 0
    for r in db.q(REJECTED_BY_REASON):
        out(f"    {r['n']:6d}  {r['kind']:9s}  {r['reason']}")
        if r["reason"].startswith("ZOMBIE"):
            zombie_writes += r["n"]
    faults = {r["point"]: r["n"] for r in db.q("SELECT point, count(*) n FROM fault_log GROUP BY 1 ORDER BY 1")}
    out(f"  stale (zombie) writes refused in total: {zombie_writes}")
    out(f"  fault points fired inside workers: {faults or 'none'}")
    started_then_cancelled = db.one("SELECT count(*) FROM jobs WHERE status = 'cancelled' AND attempts > 0")
    out(f"  cancelled after having started at least once: {started_then_cancelled}")
    max_attempts = db.one("SELECT count(*) FROM jobs WHERE error LIKE %s", ("max attempts%",))
    out(f"  failed by max attempts: {max_attempts}")
    codes = defaultdict(int)
    for answers in st.cancels.values():
        for code, _ in answers:
            codes[code] += 1
    out(f"  cancel requests by HTTP code: {dict(codes)}")
    out(f"  chaos lasted {chaos_s:.0f}s: worker kill -9 = {chaos.kills} (one every {chaos_s / max(1, chaos.kills):.1f}s), "
        f"missed kills = {chaos.kill_misses}, freezes = {chaos.pauses}, API kills = {chaos.api_kills}, "
        f"DB kills = {chaos.db_kills}, docker errors = {chaos.errors}")
    out(f"  client: 201={st.created} replay-200={st.replayed} changed-body-409={st.conflicts} gets={st.gets} "
        f"forced_timeouts={st.forced_timeouts} transport_errors={st.transport_errors} http_5xx={st.http_5xx}")
    out(f"  API latency ms (from the Windows host, through Docker's port proxy): p50={pct(st.lat, 50) * 1000:.1f} "
        f"p95={pct(st.lat, 95) * 1000:.1f} p99={pct(st.lat, 99) * 1000:.1f} max={max(st.lat) * 1000:.1f}  "
        f"({len(st.lat)} requests)")
    out(f"  submit phase: {submit_s:.1f}s = {len(st.ids) / submit_s:.0f} new jobs/s, "
        f"{len(st.lat) / submit_s:.0f} requests/s overall")
    w = db.q("SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP "
             "(ORDER BY extract(epoch FROM finished_at - created_at)) AS p FROM jobs WHERE status = 'succeeded'")[0]["p"]
    if w:
        out(f"  create->succeeded seconds: p50={w[0]:.1f} p95={w[1]:.1f} p99={w[2]:.1f}")
    out(f"  total wall time: {total_s:.1f}s")
    expected_points = [p.split(":")[0] for p in args.crash_points.split(",") if p.strip()]
    expected_points += [p.split(":")[0] + "_freeze" for p in args.freeze_points.split(",") if p.strip()]
    warnings = [(args.kill_interval > 0 and chaos.kills == 0, "no worker was killed"),
                (rerun == 0, "no job was ever re-executed (the recovery path did not run)"),
                (zombie_writes == 0, "no zombie ever tried to write (stale-write fencing was not exercised)")]
    warnings += [(faults.get(p, 0) == 0, f"fault point '{p}' never fired") for p in expected_points]
    for cond, msg in warnings:
        if cond:
            out(f"  WARNING: {msg} -- this run did not exercise that failure mode")


# ------------------------------------------------------------------- main

def compose_up(args, out):
    env = os.environ.copy()
    for b in BUGS:
        env[b] = "1" if b == args.bug else "0"
    env.update(LEASE_SECONDS=str(args.lease), CRASH_POINTS=args.crash_points, FREEZE_POINTS=args.freeze_points)
    r = docker("compose", "up", "-d", "--build", "--scale", f"worker={args.workers}", env=env)
    if r.returncode != 0:
        out(r.stdout + r.stderr)
        raise RuntimeError("docker compose up failed")
    wait_api()
    ids = {s: docker("compose", "ps", "-q", s, env=env).stdout.split() for s in ("worker", "api", "db")}
    if len(ids["worker"]) != args.workers or len(ids["api"]) != 1 or len(ids["db"]) != 1:
        raise RuntimeError(f"unexpected containers: { {k: len(v) for k, v in ids.items()} }")
    return {"worker": ids["worker"], "api": ids["api"][0], "db": ids["db"][0]}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=10000)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--kill-interval", type=float, default=2.0, help="seconds between worker kill -9; 0 disables")
    ap.add_argument("--pause-interval", type=float, default=7.0, help="seconds between worker freezes; 0 disables")
    ap.add_argument("--api-kill-interval", type=float, default=20.0, help="seconds between API kill -9; 0 disables")
    ap.add_argument("--db-kill-interval", type=float, default=0.0, help="seconds between Postgres kill -9; 0 disables")
    ap.add_argument("--crash-points", default=DEFAULT_CRASH_POINTS, help="name:probability,... ('' disables)")
    ap.add_argument("--freeze-points", default=DEFAULT_FREEZE_POINTS, help="name:probability,... ('' disables)")
    ap.add_argument("--lease", type=float, default=5.0)
    ap.add_argument("--rps", type=float, default=0, help="pace new-job POSTs at this rate; 0 = as fast as possible")
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--cancel-frac", type=float, default=0.05, help="share of jobs cancelled at a random moment")
    ap.add_argument("--cancel-running-interval", type=float, default=0.5,
                    help="every N seconds cancel 2 jobs a live worker is running; 0 disables")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--timeout", type=float, default=1800)
    ap.add_argument("--tag", default="", help="suffix for the log file name")
    ap.add_argument("--bug", choices=BUGS, default=None, help="negative control: run with one protection removed")
    args = ap.parse_args()
    if args.seed is None:
        args.seed = random.SystemRandom().randrange(10 ** 6)

    RUNS.mkdir(exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = "".join("-" + s for s in (args.bug, args.tag) if s)
    path = RUNS / f"{stamp}{suffix}.log"
    out = Out(path)
    out(f"chaos run {stamp}  args={vars(args)}")
    out(f"host: {platform.platform()}, python {platform.python_version()}, "
        f"docker {docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
    if args.bug:
        out(f"*** NEGATIVE CONTROL: {args.bug}=1. This run is EXPECTED TO FAIL. ***")
    try:
        containers = compose_up(args, out)
        db = Db()
        db.q("TRUNCATE jobs, executions, side_effects, rejected_writes, fault_log RESTART IDENTITY")
        out(f"stack up: {len(containers['worker'])} workers, tables truncated, "
            f"isolation={db.one('SHOW transaction_isolation')}, {db.one('SELECT version()').split(',')[0]}")
        rnd = random.Random(args.seed)
        keys = [f"{stamp}-{i:05d}" for i in range(args.jobs)]
        payloads = {k: make_payload(rnd, i) for i, k in enumerate(keys)}
        st = Stats()
        chaos = Chaos(containers, out, args)
        t0 = time.perf_counter()
        chaos.start()
        try:
            submit_s, timed_out = asyncio.run(run_load(args, out, st, db, chaos, keys, payloads))
        finally:
            chaos_s = time.perf_counter() - t0
            chaos.stop_and_heal()
        db.q("SELECT 1")
        wait_api()
        settle = args.lease * 2 + 3   # a worker frozen at a fault point sleeps 2 leases before it wakes up
        out(f"chaos stopped, everything resumed; waiting {settle:.0f}s so the last zombies can wake up and try to write")
        time.sleep(settle)
        settle_deadline = time.monotonic() + args.lease * 4 + 30
        while time.monotonic() < settle_deadline:
            c = db.status_counts()
            if c.get("queued", 0) + c.get("running", 0) == 0:
                break
            time.sleep(1)
        total_s = time.perf_counter() - t0

        out("--- invariants ---")
        results = check_invariants(out, db, st, keys)
        report_stats(out, db, st, chaos, args, submit_s, chaos_s, total_s)
        failed = [name for name, ok in results if not ok]
        if failed or timed_out:
            out(f"RESULT: FAIL ({len(failed)} of {len(results)} invariants violated"
                f"{', drain timed out' if timed_out else ''})")
            if args.bug:
                out("        (expected: this was a negative control, the harness caught the injected bug)")
            sys.exit(1)
        out(f"RESULT: PASS (all {len(results)} invariants hold)")
        if args.bug:
            out("        UNEXPECTED: a negative control passed. The harness did NOT detect the injected bug.")
    except SystemExit:
        raise
    except Exception as e:  # harness problem, not a verdict about the system
        out(f"HARNESS ERROR: {type(e).__name__}: {e}")
        sys.exit(2)
    finally:
        out(f"log written to {path}")


if __name__ == "__main__":
    main()
