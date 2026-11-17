# Design note — training-job queue

Written before the code. Where the implementation later diverged, the divergence is
recorded at the bottom instead of silently editing this page.

## Storage: Postgres, not SQLite

The whole point of the task is concurrency under failure. SQLite has a single writer,
so most of the races we are asked about (double claim, cancel/complete race, concurrent
retries of the same POST) cannot happen in it — tests would pass for the wrong reason.
Postgres gives row-level locks, `FOR UPDATE SKIP LOCKED`, `INSERT … ON CONFLICT`, and a
single authoritative clock (`now()`), which is what the design below relies on.
Cost: one more container. Acceptable.

## Data model (simplified)

`jobs(id, idempotency_key UNIQUE, request_hash, payload, status, attempts,
lease_owner, lease_expires_at, created_at, updated_at, finished_at, error)`

`executions(job_id, attempt, worker_id, started_at)` — one row per claim, written in
the claim transaction. Counts how many times a job *started*.

`side_effects(id, job_id, attempt, worker_id, written_at)` — the emulated "result
upload". Counts how many times a job *really took effect*. Deliberately **no** UNIQUE
on `job_id`: a unique constraint would turn a correctness bug into a loud error and
hide it from the measurement. The guarantee must come from the protocol, and the
harness must be able to see it fail (see negative controls).

## How a worker claims a job

One statement, one transaction:

```sql
UPDATE jobs SET status='running', attempts=attempts+1,
       lease_owner=$worker, lease_expires_at=now()+$lease
WHERE id = (SELECT id FROM jobs WHERE status='queued'
            ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING id, attempts;
```

`attempts` after the increment is the **fencing token**. Every later write by this
worker is conditional on `status='running' AND attempts=$token`.

## How a dead worker is detected

Leases, not liveness pings. A running worker extends its lease (heartbeat) every
`lease/5`. Every worker also runs a reaper step: running jobs with
`lease_expires_at < now()` go back to `queued` (or to `failed` after `MAX_ATTEMPTS`).
There is no special reaper process, so killing any one process cannot stop recovery.
All time comparisons use the database clock; worker clocks are never compared, so
worker clock skew cannot cause an early or late reclaim.

A heartbeat that updates 0 rows means "you lost this job" (lease expired and it was
re-claimed, or it was cancelled): the worker stops and writes nothing.

## Completion and the side effect

In **one transaction**: the fenced `UPDATE … SET status='succeeded' WHERE id=$id AND
status='running' AND attempts=$token`, and only if it updated a row, `INSERT INTO
side_effects`. Either both commit or neither does. A `kill -9` between them is a
rolled-back transaction.

## Cancelling a running job

`UPDATE jobs SET status='cancelled' WHERE id=$id AND status IN ('queued','running')`.
Cancellation takes effect immediately in the state machine. The worker finds out at
its next heartbeat or at completion, when its fenced UPDATE matches 0 rows, and it
discards its result. Cancel vs complete is decided by the row lock: whichever UPDATE
commits first wins; the second re-checks its WHERE under READ COMMITTED (Postgres
re-evaluates the predicate on the latest row version) and matches nothing.
Cancelling a terminal job is a no-op that returns the current state (409).

## Idempotent create

`INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`; on conflict, read the
existing row. Same key + same body hash → 200 with the existing job. Same key +
different body → 409. Concurrent identical POSTs are serialized by the unique index.

## Guarantee: at-least-once execution, exactly-once *effect*

A job may *run* more than once (worker killed mid-run, worker frozen past its lease).
That is unavoidable without giving up liveness. But its *effect* is committed at most
once, atomically with the terminal state, guarded by the fencing token — so every
`succeeded` job has exactly one side effect, every other job has zero.

This holds **only because the side effect lives in the same database**. A real
training job writes checkpoints to S3 / reports to an external system; those writes
cannot be fenced by a Postgres row, and become at-least-once. For those the effect
must be idempotent on `(job_id)` at the destination, or carry the fencing token and
be rejected downstream. That is the main gap between this design and production.

## What I expect to be weakest (to be checked, not assumed)

- Reaper and claim both scan `jobs`; at large queue sizes this needs the partial
  indexes on `status='queued'` / `status='running'` to stay cheap.
- A zombie's *external* work is not fenced (above).
- Single Postgres = single point of failure; no failover is designed here.

---

## Where the implementation diverged from this note (added after the code and the runs)

Everything above was written first and is left as it was. What changed while building:

1. **Server-side timeouts were added; the design above did not have them.** Testing showed
   that a worker which freezes *between its fenced UPDATE and COMMIT* keeps the job row
   locked. Final states stay correct, but while it is frozen the reaper cannot see the job
   (`SKIP LOCKED` skips it) and `cancel` waits on the lock. Every session now sets
   `idle_in_transaction_session_timeout = lease` (Postgres cuts such a session off and the
   job is recovered) and `lock_timeout = 2s` (the API answers `503 + Retry-After` instead
   of hanging). See `tests/test_silent_failures.py`, the two `frozen` tests.
2. **More bookkeeping than the note lists**: `jobs.requeues` (so that "attempts = requeues
   + 1" exposes a double claim even when fencing hides its effect), `rejected_writes`
   (every refused stale/cancelled write and why), `fault_log` (crash/freeze points).
   These exist for evidence, not for correctness.
3. **Poison jobs**: after `MAX_ATTEMPTS` (5) expired leases a job becomes `failed`
   instead of being re-queued forever. The note only mentioned this in passing.
4. **Heartbeat happens between work slices**, in the same thread as the work. A job step
   that blocks longer than one lease looks dead and is re-run; fencing makes that safe
   but wasteful. A real worker should heartbeat from a separate thread or process.
5. **`now()` is transaction-start time.** Leases are computed from it, which is fine for
   the single-statement claim/heartbeat used here, and would be subtly wrong inside a
   long transaction.
