"""All state transitions of the job queue. Every function is one statement or
one transaction, and every worker write is fenced by (status='running', attempts=token).

Time: only the database clock (now()) is ever compared. No local clock is read here.

BUG_* switches deliberately break one protection each. They exist only so the
harness and tests can prove they *detect* the corresponding bug (negative controls).
CRASH_POINTS / FREEZE_POINTS inject a fault at an exact place in the worker.
"""
import hashlib
import json
import os
import random
import signal
import time

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://queue:queue@127.0.0.1:55433/queue")
LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", "5"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))

# Postgres terminates a session that sits inside an open transaction longer than this.
# Without it, a worker that freezes between its fenced UPDATE and COMMIT keeps the row
# locked: the reaper cannot see the job and cancel() waits, for as long as it stays frozen.
IDLE_IN_TX_TIMEOUT_MS = int(float(os.environ.get("IDLE_IN_TX_TIMEOUT_SECONDS", LEASE_SECONDS)) * 1000)
# Nobody waits for a row lock longer than this; the API turns it into 503 + Retry-After.
LOCK_TIMEOUT_MS = int(float(os.environ.get("LOCK_TIMEOUT_SECONDS", "2")) * 1000)

BUG_NO_FENCING = os.environ.get("BUG_NO_FENCING") == "1"   # complete/heartbeat ignore token and status
BUG_RACY_CLAIM = os.environ.get("BUG_RACY_CLAIM") == "1"   # read-then-write claim without a lock
BUG_SPLIT_TX = os.environ.get("BUG_SPLIT_TX") == "1"       # side effect committed before the status change

CONN_KWARGS = {"autocommit": True, "row_factory": dict_row}
JOB_COLS = ("id, seq, idempotency_key, request_hash, payload, status, attempts, requeues, "
            "lease_owner, lease_expires_at, error, created_at, updated_at, finished_at")


def configure(conn, isolation="READ COMMITTED"):
    """Per-session settings. The isolation level is pinned so that a changed server or
    database default cannot silently alter the semantics the fenced UPDATEs rely on."""
    conn.execute(
        f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {isolation}; "
        f"SET idle_in_transaction_session_timeout = {IDLE_IN_TX_TIMEOUT_MS}; "
        f"SET lock_timeout = {LOCK_TIMEOUT_MS}")


def connect(dsn=None, isolation="READ COMMITTED"):
    conn = psycopg.connect(dsn or DATABASE_URL, **CONN_KWARGS)
    configure(conn, isolation)
    return conn


def request_hash(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


# -------------------------------------------------------- fault injection

def _points(spec):
    out = {}
    for part in spec.split(","):
        name, _, prob = part.strip().partition(":")
        if name:
            out[name] = float(prob or 1)
    return out


# e.g. CRASH_POINTS="claimed:0.003,mid_complete:0.003,after_complete:0.003"
#      FREEZE_POINTS="mid_complete:0.002"
# A random kill -9 almost never lands inside the few milliseconds of the completion
# transaction. These make the worker SIGKILL itself, or stop responding, exactly there.
CRASH_POINTS = _points(os.environ.get("CRASH_POINTS", ""))
FREEZE_POINTS = _points(os.environ.get("FREEZE_POINTS", ""))
FREEZE_SECONDS = float(os.environ.get("FREEZE_SECONDS", 2 * LEASE_SECONDS))


def fault_point(name, job_id, token, worker_id):
    if random.random() < CRASH_POINTS.get(name, 0):
        _log_fault(name, job_id, token, worker_id)
        os.kill(os.getpid(), getattr(signal, "SIGKILL", signal.SIGTERM))
        os._exit(137)   # only reached if the signal was ignored (PID 1 of its namespace)
    if random.random() < FREEZE_POINTS.get(name, 0):
        _log_fault(name + "_freeze", job_id, token, worker_id)
        time.sleep(FREEZE_SECONDS)


def _log_fault(point, job_id, token, worker_id):
    # On its own connection: the caller's may be inside a transaction that must not commit.
    with psycopg.connect(DATABASE_URL, autocommit=True) as c:
        c.execute("INSERT INTO fault_log (job_id, attempt, worker_id, point) VALUES (%s, %s, %s, %s)",
                  (job_id, token, worker_id, point))


# ---------------------------------------------------------------- API side

def create_job(conn, key, payload):
    """Returns (job, outcome) with outcome in created | replayed | conflict."""
    h = request_hash(payload)
    for _ in range(3):
        row = conn.execute(
            f"INSERT INTO jobs (idempotency_key, request_hash, payload) VALUES (%s, %s, %s) "
            f"ON CONFLICT (idempotency_key) DO NOTHING RETURNING {JOB_COLS}",
            (key, h, Jsonb(payload)),
        ).fetchone()
        if row:
            return row, "created"
        row = conn.execute(f"SELECT {JOB_COLS} FROM jobs WHERE idempotency_key = %s", (key,)).fetchone()
        if row:
            return row, ("replayed" if row["request_hash"] == h else "conflict")
    raise RuntimeError(f"idempotency key {key!r}: conflicting row never became visible")


def get_job(conn, job_id):
    return conn.execute(f"SELECT {JOB_COLS} FROM jobs WHERE id = %s", (job_id,)).fetchone()


def list_jobs(conn, status=None, cursor=0, limit=50):
    """Keyset pagination on seq: stable under concurrent inserts, O(limit) per page."""
    return conn.execute(
        f"SELECT {JOB_COLS} FROM jobs WHERE seq > %s AND (%s::text IS NULL OR status = %s) "
        f"ORDER BY seq LIMIT %s",
        (cursor, status, status, limit),
    ).fetchall()


def cancel_job(conn, job_id):
    """Returns (job, outcome) with outcome in cancelled | already_cancelled | terminal | not_found."""
    row = conn.execute(
        f"UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now(), "
        f"lease_owner = NULL, lease_expires_at = NULL "
        f"WHERE id = %s AND status IN ('queued', 'running') RETURNING {JOB_COLS}",
        (job_id,),
    ).fetchone()
    if row:
        return row, "cancelled"
    row = get_job(conn, job_id)
    if row is None:
        return None, "not_found"
    return row, ("already_cancelled" if row["status"] == "cancelled" else "terminal")


# ------------------------------------------------------------- worker side

def claim(conn, worker_id):
    """Atomically take the oldest queued job. Returns the job (attempts = fencing token) or None."""
    if BUG_RACY_CLAIM:
        return _claim_racy(conn, worker_id)
    return conn.execute(
        f"""
        WITH claimed AS (
            UPDATE jobs
               SET status = 'running', attempts = attempts + 1, lease_owner = %(w)s,
                   lease_expires_at = now() + make_interval(secs => %(lease)s), updated_at = now()
             WHERE id = (SELECT id FROM jobs WHERE status = 'queued'
                          ORDER BY seq FOR UPDATE SKIP LOCKED LIMIT 1)
            RETURNING {JOB_COLS}
        ), log AS (
            INSERT INTO executions (job_id, attempt, worker_id)
            SELECT id, attempts, %(w)s FROM claimed
        )
        SELECT * FROM claimed
        """,
        {"w": worker_id, "lease": LEASE_SECONDS},
    ).fetchone()


def _claim_racy(conn, worker_id):
    """NEGATIVE CONTROL: check-then-act with no lock and no status re-check."""
    row = conn.execute("SELECT id FROM jobs WHERE status = 'queued' ORDER BY seq LIMIT 1").fetchone()
    if row is None:
        return None
    time.sleep(0.005)
    job = conn.execute(
        f"UPDATE jobs SET status = 'running', attempts = attempts + 1, lease_owner = %s, "
        f"lease_expires_at = now() + make_interval(secs => %s), updated_at = now() "
        f"WHERE id = %s RETURNING {JOB_COLS}",
        (worker_id, LEASE_SECONDS, row["id"]),
    ).fetchone()
    conn.execute("INSERT INTO executions (job_id, attempt, worker_id) VALUES (%s, %s, %s)",
                 (job["id"], job["attempts"], worker_id))
    return job


def _rejected(conn, job_id, token, worker_id, kind):
    """Diagnostics only: remember that a fenced write was refused and what the row looked
    like right afterwards (cancelled? already owned by a newer attempt?)."""
    conn.execute(
        "INSERT INTO rejected_writes (job_id, attempt, worker_id, kind, seen_status, seen_attempts) "
        "SELECT %s, %s, %s, %s, status, attempts FROM jobs WHERE id = %s",
        (job_id, token, worker_id, kind, job_id))


def heartbeat(conn, job_id, token, worker_id):
    """Extend the lease. False means: you no longer own this job, stop and write nothing."""
    if BUG_NO_FENCING:
        conn.execute("UPDATE jobs SET lease_expires_at = now() + make_interval(secs => %s) WHERE id = %s",
                     (LEASE_SECONDS, job_id))
        return True
    ok = conn.execute(
        "UPDATE jobs SET lease_expires_at = now() + make_interval(secs => %s), updated_at = now() "
        "WHERE id = %s AND status = 'running' AND attempts = %s RETURNING 1",
        (LEASE_SECONDS, job_id, token),
    ).fetchone() is not None
    if not ok:
        _rejected(conn, job_id, token, worker_id, "heartbeat")
    return ok


def complete(conn, job_id, token, worker_id):
    """Terminal transition + side effect in ONE transaction, fenced by the token."""
    if BUG_SPLIT_TX:
        # NEGATIVE CONTROL: the effect is committed on its own, before the status change.
        # A crash in between leaves the job 'running'; it is re-run and the effect repeats.
        conn.execute("INSERT INTO side_effects (job_id, attempt, worker_id) VALUES (%s, %s, %s)",
                     (job_id, token, worker_id))
        fault_point("mid_complete", job_id, token, worker_id)
        return _finish(conn, job_id, token, "succeeded", None) is not None
    with conn.transaction():
        ok = _finish(conn, job_id, token, "succeeded", None) is not None
        if ok:
            conn.execute("INSERT INTO side_effects (job_id, attempt, worker_id) VALUES (%s, %s, %s)",
                         (job_id, token, worker_id))
            fault_point("mid_complete", job_id, token, worker_id)    # nothing is committed yet
    if ok:
        fault_point("after_complete", job_id, token, worker_id)      # everything is committed
    else:
        _rejected(conn, job_id, token, worker_id, "complete")
    return ok


def fail(conn, job_id, token, worker_id, error):
    ok = _finish(conn, job_id, token, "failed", error) is not None
    if not ok:
        _rejected(conn, job_id, token, worker_id, "fail")
    return ok


def _finish(conn, job_id, token, status, error):
    fence = "" if BUG_NO_FENCING else "AND status = 'running' AND attempts = %(token)s"
    return conn.execute(
        f"UPDATE jobs SET status = %(status)s, error = %(error)s, finished_at = now(), updated_at = now(), "
        f"lease_owner = NULL, lease_expires_at = NULL WHERE id = %(id)s {fence} RETURNING 1",
        {"status": status, "error": error, "id": job_id, "token": token},
    ).fetchone()


def reap(conn, batch=100):
    """Return jobs with an expired lease to the queue (or fail them after MAX_ATTEMPTS).
    Run by every worker; SKIP LOCKED keeps concurrent reapers from blocking each other."""
    return conn.execute(
        """
        UPDATE jobs
           SET status           = CASE WHEN attempts >= %(max)s THEN 'failed' ELSE 'queued' END,
               error            = CASE WHEN attempts >= %(max)s THEN 'max attempts exceeded (lease expired)' END,
               finished_at      = CASE WHEN attempts >= %(max)s THEN now() END,
               requeues         = requeues + CASE WHEN attempts >= %(max)s THEN 0 ELSE 1 END,
               lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
         WHERE id IN (SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at < now()
                       ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT %(batch)s)
        RETURNING id
        """,
        {"max": MAX_ATTEMPTS, "batch": batch},
    ).rowcount
