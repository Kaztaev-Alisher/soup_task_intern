-- Idempotent: applied by Postgres initdb and by the test suite.

CREATE TABLE IF NOT EXISTS jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seq              bigserial NOT NULL UNIQUE,          -- stable order: FIFO claim + keyset pagination
    idempotency_key  text NOT NULL UNIQUE,
    request_hash     text NOT NULL,
    payload          jsonb NOT NULL,
    status           text NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    attempts         integer NOT NULL DEFAULT 0,         -- fencing token: incremented on every claim
    requeues         integer NOT NULL DEFAULT 0,         -- incremented when the reaper returns a job to the queue
    lease_owner      text,
    lease_expires_at timestamptz,
    error            text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

CREATE INDEX IF NOT EXISTS jobs_queued_idx  ON jobs (seq)              WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (lease_expires_at) WHERE status = 'running';

-- One row per claim. "How many times did this job start running?"
CREATE TABLE IF NOT EXISTS executions (
    id         bigserial PRIMARY KEY,
    job_id     uuid NOT NULL,
    attempt    integer NOT NULL,
    worker_id  text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS executions_job_idx ON executions (job_id);

-- The emulated side effect. "How many times did this job really take effect?"
-- Deliberately NO unique constraint on job_id: a duplicate must be observable,
-- not converted into an error that hides the protocol bug.
CREATE TABLE IF NOT EXISTS side_effects (
    id         bigserial PRIMARY KEY,
    job_id     uuid NOT NULL,
    attempt    integer NOT NULL,
    worker_id  text NOT NULL,
    written_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS side_effects_job_idx ON side_effects (job_id);

-- Evidence that fencing actually fired: a worker tried to write with a stale
-- token (zombie) or after a cancel, and was refused.
CREATE TABLE IF NOT EXISTS rejected_writes (
    id        bigserial PRIMARY KEY,
    job_id    uuid NOT NULL,
    attempt   integer NOT NULL,
    worker_id text NOT NULL,
    kind      text NOT NULL,            -- heartbeat | complete | fail
    seen_status   text,                 -- what the row looked like right after the refusal
    seen_attempts integer,
    at        timestamptz NOT NULL DEFAULT now()
);

-- Fault injection log (CRASH_POINTS / FREEZE_POINTS): which attempt was killed or frozen
-- at which exact place. Lets the harness check each critical window separately.
CREATE TABLE IF NOT EXISTS fault_log (
    id        bigserial PRIMARY KEY,
    job_id    uuid NOT NULL,
    attempt   integer NOT NULL,
    worker_id text NOT NULL,
    point     text NOT NULL,            -- claimed | mid_complete | after_complete | mid_complete_freeze
    at        timestamptz NOT NULL DEFAULT now()
);
