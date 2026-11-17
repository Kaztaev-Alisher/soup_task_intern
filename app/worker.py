"""Worker process: claim -> run (heartbeating) -> fenced complete. Also reaps expired leases.

"Running" a job is emulated: sleep payload.duration_ms, then write one side_effects
row (or fail, if payload.fail is set). Run with: python -m app.worker
"""
import os
import random
import socket
import sys
import time
import uuid

import psycopg

from . import queue as q

REAP_EVERY = 1.0  # seconds, by the local monotonic clock (pacing only, never correctness)


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def run_job(conn, job, worker_id, sleep=time.sleep):
    """Returns 'done' if our terminal write was accepted, 'lost' if we were fenced out."""
    payload, token = job["payload"], job["attempts"]
    remaining = float(payload.get("duration_ms", 50)) / 1000
    beat = q.LEASE_SECONDS / 5
    while remaining > 0:
        step = min(beat, remaining)
        sleep(step)
        remaining -= step
        if remaining > 0 and not q.heartbeat(conn, job["id"], token, worker_id):
            return "lost"  # cancelled, or lease expired and someone else owns it now
    if payload.get("fail"):
        ok = q.fail(conn, job["id"], token, worker_id, "job reported failure")
    else:
        ok = q.complete(conn, job["id"], token, worker_id)
    return "done" if ok else "lost"


def loop(conn, worker_id):
    next_reap = 0.0
    while True:
        if time.monotonic() >= next_reap:
            reaped = q.reap(conn)
            if reaped:
                log(f"{worker_id} reaped {reaped} expired lease(s)")
            next_reap = time.monotonic() + REAP_EVERY
        job = q.claim(conn, worker_id)
        if job is None:
            time.sleep(random.uniform(0.05, 0.25))
            continue
        q.fault_point("claimed", job["id"], job["attempts"], worker_id)
        if run_job(conn, job, worker_id) == "lost":
            log(f"{worker_id} lost job {job['id']} attempt {job['attempts']} (fenced out)")


def main():
    # Unique per process incarnation: a restarted container is a *new* worker.
    worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    log(f"{worker_id} starting (lease={q.LEASE_SECONDS}s max_attempts={q.MAX_ATTEMPTS} "
        f"bugs: no_fencing={q.BUG_NO_FENCING} racy_claim={q.BUG_RACY_CLAIM} split_tx={q.BUG_SPLIT_TX})")
    while True:
        try:
            with q.connect() as conn:
                loop(conn, worker_id)
        except psycopg.Error as e:
            # DB down / restarting / schema not there yet: any in-flight job is simply
            # abandoned; its lease expires and it is re-run. Reconnect and carry on.
            log(f"{worker_id} database error, reconnecting: {type(e).__name__}: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
