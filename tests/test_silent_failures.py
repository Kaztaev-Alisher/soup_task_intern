"""Targeted, deterministic checks for failures that produce no error.

Each scenario is run twice: once against the real code (must hold) and once with the
relevant protection switched off (must be DETECTED). A check that cannot fail is not
evidence, so the negative controls are part of the test, not an afterthought.

Runs against a scratch database (queue_test) on the compose Postgres; no workers are
attached to it, so the tests drive every interleaving themselves.

    docker compose up -d db
    pytest tests -v -s
"""
import os
import random
import threading
import time
from pathlib import Path

import psycopg
import pytest

from app import queue as q
from app import worker

ADMIN_DSN = os.environ.get("TEST_ADMIN_DSN", "postgresql://queue:queue@127.0.0.1:55433/postgres")
TEST_DSN = ADMIN_DSN.rsplit("/", 1)[0] + "/queue_test"
SCHEMA = (Path(__file__).resolve().parent.parent / "app" / "schema.sql").read_text()
ISOLATION_LEVELS = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]


@pytest.fixture(scope="session", autouse=True)
def database():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as c:
        c.execute("DROP DATABASE IF EXISTS queue_test WITH (FORCE)")
        c.execute("CREATE DATABASE queue_test")
    with psycopg.connect(TEST_DSN, autocommit=True) as c:
        c.execute(SCHEMA)


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    for flag in ("BUG_NO_FENCING", "BUG_RACY_CLAIM", "BUG_SPLIT_TX"):
        monkeypatch.setattr(q, flag, False)
    with psycopg.connect(TEST_DSN, autocommit=True) as c:
        c.execute("TRUNCATE jobs, executions, side_effects, rejected_writes, fault_log RESTART IDENTITY")


def conn(isolation="READ COMMITTED"):
    return q.connect(TEST_DSN, isolation)


def seed(n):
    with conn() as c:
        return [q.create_job(c, f"k{i}", {"n": i})[0]["id"] for i in range(n)]


def scalar(sql, *params):
    with conn() as c:
        return list(c.execute(sql, params).fetchone().values())[0]


def effects(job_id):
    return scalar("SELECT count(*) FROM side_effects WHERE job_id = %s", job_id)


def expire_lease(job_id):
    """What a frozen/partitioned worker looks like from the database's point of view."""
    with conn() as c:
        c.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s", (job_id,))


# ------------------------------------------------------------ double claim

def claim_everything(n_jobs, n_threads, isolation):
    seed(n_jobs)
    claims, serialization_errors = [], [0]
    lock, barrier = threading.Lock(), threading.Barrier(n_threads)

    def run(i):
        with conn(isolation) as c:
            barrier.wait()
            empty = 0
            while empty < 3:
                try:
                    job = q.claim(c, f"w{i}")
                except psycopg.errors.SerializationFailure:
                    with lock:
                        serialization_errors[0] += 1
                    continue
                if job is None:
                    empty += 1
                    time.sleep(0.01)
                else:
                    empty = 0
                    with lock:
                        claims.append(job["id"])

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n_threads)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return claims, serialization_errors[0]


@pytest.mark.parametrize("isolation", ISOLATION_LEVELS)
def test_double_claim_never_happens(isolation):
    claims, errors = claim_everything(300, 16, isolation)
    print(f"\n  [{isolation}] 300 jobs, 16 claimers: {len(claims)} claims, "
          f"{len(set(claims))} distinct, {errors} serialization errors (loud, retried)")
    assert len(claims) == 300 and len(set(claims)) == 300
    assert scalar("SELECT count(*) FROM jobs WHERE attempts <> 1 OR status <> 'running'") == 0
    assert scalar("SELECT count(*) FROM executions") == 300


def test_double_claim_is_detected_when_claim_is_racy(monkeypatch):
    """Negative control: check-then-act claim. The invariant used by the harness
    (attempts = requeues + 1) must light up."""
    monkeypatch.setattr(q, "BUG_RACY_CLAIM", True)
    claims, _ = claim_everything(50, 16, "READ COMMITTED")
    doubly = scalar("SELECT count(*) FROM jobs WHERE attempts <> requeues + 1")
    print(f"\n  racy claim: {len(claims)} claims for 50 jobs, {doubly} jobs claimed more than once")
    assert len(claims) > 50 and doubly > 0


# ------------------------------------------------------------------ zombie

def zombie_scenario():
    """A claims; A freezes past its lease; reaper requeues; B claims and completes;
    A wakes up and tries to heartbeat and complete with its stale token."""
    [job_id] = seed(1)
    with conn() as a, conn() as b:
        job_a = q.claim(a, "A")
        expire_lease(job_id)
        assert q.reap(b) == 1
        job_b = q.claim(b, "B")
        assert (job_a["attempts"], job_b["attempts"]) == (1, 2)
        assert q.complete(b, job_id, job_b["attempts"], "B") is True
        zombie_heartbeat = q.heartbeat(a, job_id, job_a["attempts"], "A")
        zombie_complete = q.complete(a, job_id, job_a["attempts"], "A")
    return job_id, zombie_heartbeat, zombie_complete


def test_zombie_write_is_refused():
    job_id, hb, done = zombie_scenario()
    assert hb is False and done is False
    assert effects(job_id) == 1
    assert scalar("SELECT attempt FROM side_effects WHERE job_id = %s", job_id) == 2
    assert scalar("SELECT count(*) FROM rejected_writes WHERE job_id = %s", job_id) == 2


def test_zombie_write_before_the_new_owner_finishes():
    """Same, but the zombie wakes up while B is still running: it must not finish B's job."""
    [job_id] = seed(1)
    with conn() as a, conn() as b:
        job_a = q.claim(a, "A")
        expire_lease(job_id)
        q.reap(b)
        job_b = q.claim(b, "B")
        assert q.complete(a, job_id, job_a["attempts"], "A") is False
        assert scalar("SELECT status FROM jobs WHERE id = %s", job_id) == "running"
        assert q.complete(b, job_id, job_b["attempts"], "B") is True
    assert effects(job_id) == 1


def test_zombie_duplicate_is_detected_without_fencing(monkeypatch):
    monkeypatch.setattr(q, "BUG_NO_FENCING", True)
    job_id, _, done = zombie_scenario()
    print(f"\n  no fencing: zombie complete accepted={done}, side effects={effects(job_id)}")
    assert done is True and effects(job_id) == 2


def test_worker_that_wakes_up_late_stops_itself():
    """End to end through worker.run_job: the freeze happens inside the job's sleep."""
    [job_id] = seed(1)
    with conn() as a, conn() as b:
        job_a = q.claim(a, "A")
        job_a["payload"] = {"duration_ms": 3 * q.LEASE_SECONDS * 1000}

        def frozen_sleep(_seconds):          # while A "sleeps", the world moves on
            expire_lease(job_id)
            q.reap(b)
            q.claim(b, "B")

        assert worker.run_job(a, job_a, "A", sleep=frozen_sleep) == "lost"
    assert effects(job_id) == 0
    assert scalar("SELECT lease_owner FROM jobs WHERE id = %s", job_id) == "B"


# ---------------------------------------------------- cancel/complete race

def race_cancel_vs_complete(rounds, isolation="READ COMMITTED"):
    ids = seed(rounds)
    outcomes = {"cancel_won": 0, "complete_won": 0, "broken": [], "serialization_errors": 0}
    with conn(isolation) as w, conn(isolation) as api:
        for job_id in ids:
            job = None
            while job is None or job["id"] != job_id:
                job = q.claim(w, "W")
            barrier, res = threading.Barrier(2), {}

            def do_complete():
                barrier.wait()
                try:
                    res["complete"] = q.complete(w, job_id, job["attempts"], "W")
                except psycopg.errors.SerializationFailure:
                    res["complete"] = "error"

            def do_cancel():
                barrier.wait()
                time.sleep(random.uniform(0, 0.002))   # jitter, so that both sides get to win
                try:
                    res["cancel"] = q.cancel_job(api, job_id)[1]
                except psycopg.errors.SerializationFailure:
                    res["cancel"] = "error"

            ts = [threading.Thread(target=do_complete), threading.Thread(target=do_cancel)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            status, n = scalar("SELECT status FROM jobs WHERE id = %s", job_id), effects(job_id)
            outcomes["serialization_errors"] += list(res.values()).count("error")
            if "error" in res.values():
                with conn() as fix:   # a loud error: caller would retry; tidy up so the loop can go on
                    q.cancel_job(fix, job_id)
                continue
            if res["cancel"] == "cancelled" and res["complete"] is False and status == "cancelled" and n == 0:
                outcomes["cancel_won"] += 1
            elif res["cancel"] == "terminal" and res["complete"] is True and status == "succeeded" and n == 1:
                outcomes["complete_won"] += 1
            else:
                outcomes["broken"].append((str(job_id), res, status, n))
    return outcomes


@pytest.mark.parametrize("isolation", ISOLATION_LEVELS)
def test_cancel_complete_race_has_exactly_one_winner(isolation):
    o = race_cancel_vs_complete(300, isolation)
    print(f"\n  [{isolation}] 300 races: cancel won {o['cancel_won']}, complete won {o['complete_won']}, "
          f"serialization errors {o['serialization_errors']}, inconsistent {len(o['broken'])}")
    assert o["broken"] == []
    if isolation == "READ COMMITTED":
        assert o["serialization_errors"] == 0
        # non-vacuity: both orders of the race must actually have been exercised
        assert o["cancel_won"] > 0 and o["complete_won"] > 0


def test_cancel_complete_race_is_detected_without_fencing(monkeypatch):
    monkeypatch.setattr(q, "BUG_NO_FENCING", True)
    o = race_cancel_vs_complete(300)
    print(f"\n  no fencing: {len(o['broken'])} of 300 races ended inconsistent, e.g. {o['broken'][:1]}")
    assert len(o["broken"]) > 0


# ------------------------------------------------- duplicate on POST retry

def test_concurrent_identical_creates_make_one_job():
    n, barrier, results, lock = 32, threading.Barrier(32), [], threading.Lock()

    def run():
        with conn() as c:
            barrier.wait()
            job, outcome = q.create_job(c, "same-key", {"lr": 0.1})
            with lock:
                results.append((str(job["id"]), outcome))

    threads = [threading.Thread(target=run) for _ in range(n)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    outcomes = [o for _, o in results]
    print(f"\n  32 concurrent creates: {outcomes.count('created')} created, {outcomes.count('replayed')} replayed")
    assert len({i for i, _ in results}) == 1
    assert outcomes.count("created") == 1 and outcomes.count("replayed") == n - 1
    assert scalar("SELECT count(*) FROM jobs") == 1


def test_same_key_different_body_is_a_conflict_and_changes_nothing():
    with conn() as c:
        job, _ = q.create_job(c, "key", {"lr": 0.1})
        other, outcome = q.create_job(c, "key", {"lr": 0.2})
    assert outcome == "conflict" and other["id"] == job["id"]
    assert scalar("SELECT payload->>'lr' FROM jobs") == "0.1"


def test_retry_after_the_job_already_finished_does_not_rerun_it():
    with conn() as c:
        job, _ = q.create_job(c, "key", {"lr": 0.1})
        claimed = q.claim(c, "W")
        q.complete(c, claimed["id"], claimed["attempts"], "W")
        again, outcome = q.create_job(c, "key", {"lr": 0.1})
        assert outcome == "replayed" and again["status"] == "succeeded"
        assert q.claim(c, "W") is None
    assert effects(job["id"]) == 1


# ---------------------------------------------------------- isolation level

def test_sessions_pin_read_committed_even_if_the_database_default_changes():
    with psycopg.connect(TEST_DSN, autocommit=True) as c:
        c.execute("ALTER DATABASE queue_test SET default_transaction_isolation = 'serializable'")
    try:
        with psycopg.connect(TEST_DSN, autocommit=True) as raw:
            assert raw.execute("SHOW transaction_isolation").fetchone()[0] == "serializable"
        with conn() as c:
            assert scalar_on(c, "SHOW transaction_isolation") == "read committed"
            with c.transaction():
                assert scalar_on(c, "SHOW transaction_isolation") == "read committed"
    finally:
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            c.execute("ALTER DATABASE queue_test RESET default_transaction_isolation")


def scalar_on(c, sql):
    return list(c.execute(sql).fetchone().values())[0]


# ---------------------------------------------------------------- clock skew

def test_lease_is_computed_on_the_database_clock(monkeypatch):
    """Worker clock one year ahead: lease length and job outcome must not change."""
    year = 365 * 86400
    real_time, real_mono = time.time, time.monotonic
    monkeypatch.setattr(time, "time", lambda: real_time() + year)
    monkeypatch.setattr(time, "monotonic", lambda: real_mono() + year)
    [job_id] = seed(1)
    with conn() as c:
        job = q.claim(c, "skewed")
        remaining = scalar("SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs WHERE id = %s", job_id)
        assert 0 < float(remaining) <= q.LEASE_SECONDS
        assert q.reap(c) == 0                       # not reaped early
        assert worker.run_job(c, job, "skewed", sleep=lambda s: None) == "done"
    assert effects(job_id) == 1


def test_no_wall_clock_in_queue_logic():
    """Static guard: correctness code must not read a local wall clock at all."""
    src = (Path(q.__file__).read_text() + Path(worker.__file__).read_text())
    for forbidden in ("time.time(", "datetime.now", "datetime.utcnow", "date.today"):
        assert forbidden not in src, f"local clock used: {forbidden}"


def test_database_clock_jump_costs_a_rerun_but_not_a_duplicate():
    """If the DB clock jumps forward, live leases expire early. That must degrade to
    'job runs twice', never to 'effect happens twice'."""
    [job_id] = seed(1)
    with conn() as a, conn() as b:
        job_a = q.claim(a, "A")
        expire_lease(job_id)                         # equivalent to now() jumping past the lease
        q.reap(b)
        job_b = q.claim(b, "B")
        assert q.complete(a, job_id, job_a["attempts"], "A") is False   # A was healthy, still loses
        assert q.complete(b, job_id, job_b["attempts"], "B") is True
    assert effects(job_id) == 1 and scalar("SELECT count(*) FROM executions") == 2


# ------------------------------------------------------- crash-atomicity

def test_crash_between_status_change_and_side_effect_leaves_neither():
    """kill -9 in the middle of complete() == the transaction never commits."""
    [job_id] = seed(1)
    c = conn()
    job = q.claim(c, "W")
    with pytest.raises(RuntimeError):
        with c.transaction():
            assert q._finish(c, job_id, job["attempts"], "succeeded", None) is not None
            raise RuntimeError("simulated crash before the side effect insert")
    c.close()
    assert scalar("SELECT status FROM jobs WHERE id = %s", job_id) == "running"
    assert effects(job_id) == 0


def test_reaper_gives_up_after_max_attempts():
    [job_id] = seed(1)
    with conn() as c:
        for _ in range(q.MAX_ATTEMPTS):
            assert q.claim(c, "W") is not None
            expire_lease(job_id)
            q.reap(c)
        assert q.claim(c, "W") is None
    assert scalar("SELECT status FROM jobs WHERE id = %s", job_id) == "failed"
    assert scalar("SELECT attempts - requeues FROM jobs WHERE id = %s", job_id) == 1
    assert effects(job_id) == 0


def test_refused_writes_record_why():
    """rejected_writes keeps what the row looked like, so a run can tell zombie writes
    (a newer attempt owns the job) from writes refused because of a cancel."""
    job_id, _, _ = zombie_scenario()
    assert scalar("SELECT count(*) FROM rejected_writes WHERE job_id = %s AND seen_attempts > attempt", job_id) == 2
    with conn() as c:
        q.create_job(c, "cancel-me", {})
        claimed = q.claim(c, "W")
        q.cancel_job(c, claimed["id"])
        assert q.complete(c, claimed["id"], claimed["attempts"], "W") is False
    assert scalar("SELECT seen_status FROM rejected_writes WHERE job_id = %s", claimed["id"]) == "cancelled"


# ------------------------------- frozen inside the completion transaction

def freeze_inside_complete(monkeypatch, idle_timeout_ms):
    """Worker A passes the fence (UPDATE done, row locked, side effect inserted) and then
    stops responding for 3 s before COMMIT - through the real complete() and its freeze
    point. Returns what the rest of the system could and could not do meanwhile."""
    monkeypatch.setattr(q, "DATABASE_URL", TEST_DSN)             # where the fault log goes
    monkeypatch.setattr(q, "FREEZE_POINTS", {"mid_complete": 1.0})
    monkeypatch.setattr(q, "FREEZE_SECONDS", 3.0)
    monkeypatch.setattr(q, "IDLE_IN_TX_TIMEOUT_MS", idle_timeout_ms)
    [job_id] = seed(1)
    seen = {}
    a = conn()
    job = q.claim(a, "A")
    expire_lease(job_id)

    def frozen_worker():
        try:
            seen["zombie_complete"] = q.complete(a, job_id, job["attempts"], "A")
        except psycopg.Error as e:
            seen["zombie_complete"] = "error: " + type(e).__name__

    t = threading.Thread(target=frozen_worker)
    t.start()
    time.sleep(0.5)                                   # A is now asleep inside its transaction
    with conn() as b:
        b.execute("SET lock_timeout = '300ms'")
        seen["reaped_while_frozen"] = q.reap(b)
        try:
            seen["cancel_while_frozen"] = q.cancel_job(b, job_id)[1]
        except psycopg.errors.LockNotAvailable:
            seen["cancel_while_frozen"] = "refused after lock_timeout"
        time.sleep(1.7)                               # ~2.5 s in: A is still asleep
        seen["reaped_2s_later"] = q.reap(b)
    t.join()
    a.close()
    seen["status"] = scalar("SELECT status FROM jobs WHERE id = %s", job_id)
    seen["effects"] = effects(job_id)
    return seen


def test_worker_frozen_inside_the_completion_transaction_is_cut_off(monkeypatch):
    seen = freeze_inside_complete(monkeypatch, idle_timeout_ms=1500)
    print(f"\n  idle_in_transaction_session_timeout = 1.5 s: {seen}")
    assert seen["reaped_while_frozen"] == 0                               # row is locked: invisible to the reaper
    assert seen["cancel_while_frozen"] == "refused after lock_timeout"    # a bounded wait, not a hang
    assert seen["reaped_2s_later"] == 1                                   # Postgres cut the session off; job recovered
    assert str(seen["zombie_complete"]).startswith("error")              # the zombie's COMMIT fails
    assert (seen["status"], seen["effects"]) == ("queued", 0)


def test_without_the_timeout_a_frozen_worker_holds_its_job_hostage(monkeypatch):
    """Negative control for the timeout. The final state is still correct (one effect),
    which is exactly why a final-state harness cannot see this. What is lost is liveness:
    for as long as the worker stays frozen, the job cannot be reaped or cancelled."""
    seen = freeze_inside_complete(monkeypatch, idle_timeout_ms=0)
    print(f"\n  timeout disabled: {seen}")
    assert seen["reaped_while_frozen"] == 0
    assert seen["cancel_while_frozen"] == "refused after lock_timeout"
    assert seen["reaped_2s_later"] == 0                                   # still hostage
    assert seen["zombie_complete"] is True
    assert (seen["status"], seen["effects"]) == ("succeeded", 1)
