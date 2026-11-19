# Training-job queue — will it survive production?

A job queue (Postgres, FastAPI, Python workers) and the evidence for and against it.

**Short answer: DON'T SHIP yet at 100 requests/s.** The correctness core held in every
run under every fault injected (8 clean chaos runs, 63,000 jobs, 948 worker `kill -9`,
27 Postgres `kill -9`, 0 violations of 16 invariants; 10 of 10 negative controls caught).
What is not ready is everything around it: one API process, one database, no alerting,
and a guarantee that stops at the database boundary. Details in [Verdict](#verdict).

- Design note, written before the code: [DESIGN.md](DESIGN.md)
- Raw output of every run, including failed, superseded and disturbed ones: [runs/](runs/)

## Run it

```bash
docker compose up -d --build        # Postgres + API (:8000) + 4 workers
```

```bash
curl -X POST http://127.0.0.1:8000/jobs -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" -d '{"payload": {"duration_ms": 200}}'
curl http://127.0.0.1:8000/jobs/<id>
curl "http://127.0.0.1:8000/jobs?status=succeeded&limit=50&cursor=0"
curl -X POST http://127.0.0.1:8000/jobs/<id>/cancel
```

`POST /jobs`: 201 created, 200 same key and body (replay), 409 same key with another body.
`cancel`: 200 the job is cancelled, 409 it had already finished. Any endpoint may answer
503 + `Retry-After` (database down, or a row lock not granted in 2 s); all are safe to retry.
Host ports can be moved with `API_PORT` / `DB_PORT` (the database is published on 55433).

Tests, harness and load test run on the host (Python 3.12+):

```bash
python -m venv .venv
.venv/Scripts/pip install -r harness/requirements.txt     # Linux/macOS: .venv/bin/pip
.venv/Scripts/python -m pytest tests -v -s                # 24 targeted silent-failure tests, ~3 min
.venv/Scripts/python harness/chaos.py                     # 10,000 jobs, 8 workers, full chaos, ~5 min
.venv/Scripts/python harness/chaos.py --bug BUG_SPLIT_TX  # negative control: expected to FAIL
.venv/Scripts/python harness/summarize.py                 # rebuild the run table from runs/*.log
docker compose exec -T -e RPS=100 -e DURATION=60 api python - < harness/loadtest.py
```

(PowerShell has no `<`: `Get-Content harness/loadtest.py | docker compose exec -T -e RPS=100 -e DURATION=60 api python -`.)

| path | what |
|---|---|
| `app/queue.py` | every state transition, one statement or one transaction each |
| `app/worker.py` | claim → run (heartbeat) → fenced complete; reaps expired leases |
| `app/api.py` | create / get / list / cancel |
| `app/schema.sql` | `jobs`, `executions`, `side_effects` + evidence tables |
| `tests/test_silent_failures.py` | each silent failure, with and without its protection |
| `harness/chaos.py` | the chaos harness and its 16 invariants |
| `harness/loadtest.py` | open-loop load test, run inside the Docker network |
| `harness/summarize.py` | builds the table below from the raw logs |

## Design in six lines

- **Claim**: one `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED LIMIT 1)`; `attempts` is incremented and is the **fencing token**.
- **Dead worker**: leases (5 s) extended by heartbeats; any worker returns expired jobs to the queue. Only the database clock is compared.
- **Completion**: the status change and the side effect commit in one transaction, guarded by `status='running' AND attempts=token`.
- **Cancel**: takes effect in the state machine immediately; the worker's next fenced write matches nothing and it discards its result.
- **Create**: `INSERT … ON CONFLICT (idempotency_key) DO NOTHING`, then compare the body hash.
- **Guarantee**: at-least-once execution, exactly-once *effect* — true only because the effect lives in the same database (see Verdict).

## What the chaos harness does

10,000 jobs through the HTTP API (20–150 ms each, 1% lasting 2–7 s, 2% failing by
design) against 8 worker containers, while:

| fault | how | how often |
|---|---|---|
| worker `kill -9` | `docker kill -s KILL`, restarted 0.2–1 s later | every ~2 s |
| worker freeze (zombie) | `docker pause` for 1.4–2.2 leases | every ~7 s |
| crash points | the worker SIGKILLs itself right after a claim, inside the completion transaction, right after its COMMIT | 0.3% of jobs each |
| freeze point | the worker stops responding for 2 leases inside the completion transaction | 0.2% of jobs |
| API `kill -9` | not asked for; makes "retry a POST whose answer was lost" real | every ~20 s |
| Postgres `kill -9` | only in the runs marked "DB kills" | every ~30 s |
| request retries | 15% of keys POSTed 3× concurrently, 15% replayed later (a tenth with a changed body), 1% abandoned after 1 ms and retried blind | |
| cancels | 5% of jobs at a random moment, plus 2 jobs a live worker is running every 0.5 s | |

When the queue is drained the chaos stops, zombies get 13 s to wake up and try to write,
and the invariants are checked in the database:

- **I1** every acknowledged key is in the DB with the id the client got · **I2** one row per key
- **I3** nothing left queued/running
- **I4** no job has more than one side effect · **I5** succeeded ⇔ exactly one · **I6** failed/cancelled ⇒ none · **I7** the effect came from the last attempt
- **I8** no double claim: `attempts = requeues + 1`, one `executions` row per attempt
- **I9** terminal rows hold no lease · **I10** cancel is honest (200 ⇒ ends cancelled; 409 ⇒ ended as reported) · **I11** outcome matches the job
- **I12** idempotent create as seen by the client · **I13** pagination returns every job exactly once
- **I14** an attempt killed before its COMMIT left no effect · **I15** an attempt killed right after its COMMIT is final and is never re-run · **I16** an attempt frozen inside its completion transaction never commits

Each log ends with a section "what the run actually exercised": how many jobs were
re-run, how many stale writes were refused and why, which fault points fired, and a
WARNING for every failure mode the run did not exercise. That section exists because a
green run that exercised nothing proves nothing — and it caught exactly that (below).

## Results: every run

24 chaos runs. This table is generated by `harness/summarize.py` from the logs in `runs/`.

| log file | harness | what | jobs | worker kill -9 | freezes | API kills | DB kills | fault points fired | jobs re-run | zombie writes refused | seconds | result |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261116-155404.log | v1 | clean | 500 | 9 | 2 | 0 | 0 | 0 | 2 | - | 55.7 | PASS |
| 20261116-160011.log | v1 | clean | 1000 | 6 | 2 | 0 | 0 | 0 | 1 | - | 44.9 | PASS |
| 20261116-160410.log | v1 | clean | 10000 | 43 | 9 | 0 | 0 | 0 | 9 | - | 186.2 | PASS |
| 20261116-160727.log | v1 | clean | 10000 | 43 | 11 | 0 | 0 | 0 | 7 | - | 186.2 | PASS |
| 20261116-161042.log | v1 | clean | 10000 | 46 | 10 | 0 | 0 | 0 | 13 | - | 190.3 | PASS |
| 20261116-161402-BUG_NO_FENCING.log | v1 | control: BUG_NO_FENCING | 3000 | 15 | 4 | 0 | 0 | 0 | 2 | - | 75.8 | FAIL I4,I5,I7,I9,I10 |
| 20261116-161535-BUG_RACY_CLAIM.log | v1 | control: BUG_RACY_CLAIM | 3000 | 30 | 7 | 0 | 0 | 0 | 1937 | - | 130.2 | FAIL I8 |
| 20261116-161801-BUG_SPLIT_TX.log | v1 | control: BUG_SPLIT_TX | 3000 | 19 | 5 | 0 | 0 | 0 | 4 | - | 95.1 | FAIL I4,I5,I6,I7 |
| 20261116-161953.log | v1 | clean, paced 100 jobs/s | 10000 | 40 | 10 | 0 | 0 | 0 | 0 | - | 180.1 | PASS |
| 20261116-164127-smoke.log | v2 | clean | 1500 | 26 | 8 | 2 | 0 | 12 | 20 | 2 | 69.8 | PASS |
| 20261116-164317-smoke-dbkill.log | v2 | clean, DB kills | 1500 | 33 | 8 | 4 | 6 | 16 | 38 | 0 | 92.7 | PASS |
| 20261116-164531.log | v2 | clean | 10000 | 138 | 39 | 12 | 0 | 80 | 146 | 22 | 295.3 | PASS |
| 20261116-165037.log | v2 | clean | 10000 | 139 | 36 | 11 | 0 | 107 | 160 | 24 | 295.9 | PASS |
| 20261116-165545.log | v2 | clean | 10000 | 148 | 39 | 14 | 0 | 100 | 152 | 22 | 320.5 | PASS |
| 20261116-170118-rps100.log | v2 | clean, paced 100 jobs/s | 10000 | 140 | 42 | 16 | 0 | 94 | 164 | 25 | 311.5 | PASS |
| 20261116-170642-dbkill.log | v2 | clean, DB kills | 10000 | 155 | 47 | 17 | 9 | 96 | 179 | 15 | 373.2 | PASS |
| 20261116-171307-BUG_NO_FENCING.log | v2 | control: BUG_NO_FENCING | 3000 | 47 | 14 | 5 | 0 | 28 | 41 | 0 | 119.3 | FAIL I4,I5,I7,I9,I10 |
| 20261116-171524-BUG_RACY_CLAIM.log | v2 | control: BUG_RACY_CLAIM | 3000 | 74 | 20 | 8 | 0 | 36 | 1554 | 1972 | 169.9 | FAIL I8 |
| 20261116-171831-BUG_SPLIT_TX.log | v2 | control: BUG_SPLIT_TX | 3000 | 48 | 14 | 5 | 0 | 26 | 51 | 3 | 125.3 | FAIL I4,I5,I6,I7,I14,I16 |
| 20261116-172054-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 110 | 0 | 0 | 0 | 0 | 85 | 0 | 244.3 | FAIL I4,I5,I7 |
| 20261116-172517-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 122 | 0 | 0 | 0 | 0 | 102 | 0 | 260.3 | FAIL I4,I5,I7 |
| 20261116-172948-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 116 | 0 | 0 | 0 | 0 | 92 | 1 | 1345.6 | FAIL I4,I5,I7 |
| 20261117-110515-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 112 | 0 | 0 | 0 | 0 | 82 | 0 | 239.6 | FAIL I4,I5,I7 |
| 20261117-110933-dbkill.log | v2 | clean, DB kills | 10000 | 169 | 50 | 16 | 12 | 111 | 188 | 11 | 381.9 | PASS |

- **Clean runs, final harness (v2): 8 of 8 PASS** — 63,000 jobs, 948 worker kills, 269 freezes, 92 API kills, 27 Postgres kills, 616 fault points fired (185 after a claim, 167 inside the completion transaction, 183 right after COMMIT, 81 freezes inside the transaction), 1,047 jobs executed more than once, 121 zombie writes refused by fencing.
- **Clean runs, first harness (v1): 6 of 6 PASS**, but superseded: v1 had flaws that made its green weaker than it looked (next section). Kept, not counted.
- **Negative controls: 10 of 10 FAIL**, as they must.
- Other raw output in `runs/`: three pytest runs (21, 4 and 24 tests, all passed), five load-test measurements, and a classification of the Postgres server log.

### Runs that are not what they seem — read before trusting the table

1. **All v1 runs** (first nine rows). Workers were killed every ~4 s, not ~2 s as the
   task says: the restart ran in the killer's loop and stretched it. And the thread that
   cancels running jobs picked *any* `running` job; orphans of killed workers stay
   `running` for a whole lease, so it cancelled about 90% of them before they could be
   requeued. In `20261116-161953.log` 40 workers were killed and **not one job was ever
   re-executed** — the run was green and had not tested recovery at all. The harness's
   own warning ("no job was ever re-executed") is what exposed it. Fixed in v2:
   start-to-start ticks, and cancels aimed only at jobs a live worker touched in the last 400 ms.
2. **`20261116-155404.log`** used `httpx.AsyncClient`, which on this Windows host managed
   ~40 requests/s against an API that serves ~330/s to plain threads. Its latency numbers measure the client.
3. **`20261116-172948-…-killonly.log`**: the laptop went to sleep for 18 minutes in the
   middle (a 1,101 s hole in the log; Postgres logged one checkpoint "writing" for 1,143 s).
   Its verdict stands, its timings do not. Repeated as `20261117-110515`.
4. **`20261116-170642-dbkill.log`**: one `docker kill` returned an error although the
   container had died; the harness treated it as a miss and did not restart that worker,
   so the last two minutes ran on 7 workers. Invariants held. Fixed and repeated as `20261117-110933`.

## Negative controls: can the harness see a bug at all?

Each control removes one protection (`BUG_*` environment variable) and must fail.

| removed | what the harness saw (v2 run) | what it means |
|---|---|---|
| fencing (`BUG_NO_FENCING`) | 5 jobs with two side effects (I4, I5, I7), 112 cancels acknowledged with 200 for jobs that ended `succeeded` (I10) | zombies and cancel races write freely |
| atomic claim (`BUG_RACY_CLAIM`) | **only I8**: 1,548 of 3,000 jobs claimed more than once | see below |
| one transaction (`BUG_SPLIT_TX`) | 20 duplicated effects (I4), 91 effects on cancelled jobs (I6), all 11 crashes inside "the transaction" left an effect behind (I14) | effect and status can diverge |

The racy-claim control is the instructive one. With a broken claim half the jobs ran two
to seven times, and **every final state was still correct**: fencing refused 1,972 stale
writes and no effect was duplicated. In production this bug would be completely silent —
the only symptoms are wasted compute and, in this run, two jobs wrongly failed for
"max attempts". It is visible only because `attempts = requeues + 1` is checked.

Not controlled: idempotent create. Its protection is a UNIQUE constraint, which no flag
switches off, so I1/I2/I12 have never been shown to fail.

## Is 10,000 jobs enough? No — the count that matters is faults per window

A random `kill -9` almost never lands in the few milliseconds where it matters. To
measure that, `BUG_SPLIT_TX` (effect committed just before the status change) was run
with **kills only**: no crash points, no freezes, no cancels.

| run | worker kills | duplicated effects found |
|---|---|---|
| 20261116-172054 | 110 | 2 |
| 20261116-172517 | 122 | 1 |
| 20261116-172948 (disturbed by sleep) | 116 | 3 |
| 20261117-110515 | 112 | 3 |

9 hits in 460 kills: a random kill lands in that window about **2% of the time**. All
four runs caught the bug, by a margin of one to three jobs out of 10,000. At that rate a
run with ~115 kills misses it about 1 time in 10, and a run with the 43 kills of the
first harness would have missed it about 4 times in 10. Ten thousand jobs says nothing
by itself; what counts is how many faults landed in each critical window. That is why
crash points exist: every 10,000-job v2 run puts 20–33 kills *inside* the completion
transaction and 16–38 right after its COMMIT by construction (I14, I15), instead of two
or three by luck.

## Silent failures: how each was checked

Tests are in `tests/test_silent_failures.py`; each scenario runs against the real code
and again with its protection removed, where one exists.

| failure | check | result | with the protection removed |
|---|---|---|---|
| **Double claim** | 16 threads claim 300 jobs, at three isolation levels; chaos I8 | 300 claims, 300 distinct; I8 held in 8/8 runs | 379 claims for 50 jobs; chaos fails I8 |
| **Zombie writes a stale result** | 3 tests (zombie wakes after, and before, the new owner finishes; and through `run_job`); chaos freezes | stale write refused, one effect; 121 zombie writes refused in clean runs | zombie's write accepted, 2 effects; chaos fails I4 |
| **Cancel / complete race** | 300 races, both threads released by a barrier; chaos aims ~1,000 cancels per run at running jobs (I10) | cancel won 162, complete won 138, 0 inconsistent | 171 of 300 inconsistent; chaos fails I10 |
| **Duplicate on request retry** | 32 concurrent identical creates; replays; blind retries after a 1 ms timeout (64–94 per 10,000-job run); API kills (6,500–11,400 transport errors retried per run) | 1 created, 31 replayed; I1, I2, I12 held | no control (see above) |
| **Transaction isolation level** | sessions pin READ COMMITTED (tested against a changed database default); double-claim and race tests repeated under REPEATABLE READ and SERIALIZABLE | no inconsistency at any level | under RR/SERIALIZABLE the same code raises 140–2,458 serialization errors in the claim test and one in almost every race: loud, and workers do not retry them — do not run this at a stricter level |
| **Clock skew** | worker clock set one year ahead; static guard that queue code reads no wall clock; database clock jump simulated by expiring a live lease | lease unaffected; a clock jump costs a re-run, never a second effect | — |
| **Crash inside / after COMMIT** | crash points; I14, I15 | 167 kills inside the transaction left no effect; 183 kills right after COMMIT were final | `BUG_SPLIT_TX`: fails I14 |
| **Frozen inside the completion transaction** (found during this work) | 2 tests; freeze point; I16 | see below | job held hostage |

### The one this design did not anticipate

A worker that freezes *between its fenced UPDATE and COMMIT* holds the row lock. The
final state is still right, so no final-state harness can see it, but while the worker
stays frozen the reaper cannot see the job (`SKIP LOCKED` skips it) and `cancel` waits on
the lock. `test_without_the_timeout_a_frozen_worker_holds_its_job_hostage` shows it:
reaper finds 0 jobs, cancel is refused, and 2 s later nothing has changed. Fixed with two
session settings: `idle_in_transaction_session_timeout = lease` (Postgres cuts the
session off; the job is recovered) and `lock_timeout = 2 s` (the API answers 503 instead
of hanging). It was not hypothetical: the Postgres log for the test run and chaos series 2
(`runs/postgres-server-log-classes-after-series-2.txt`) shows 50 sessions cut off by that
timeout and 18 statements stopped by the lock timeout (one and two of those are the
tests themselves). The same file shows 191 clients that vanished with a transaction
open — all rolled back — and that all 3,198 serialization errors came from the
deliberate RR/SERIALIZABLE tests, none from chaos.

## What the harness does NOT catch

- **Network partitions.** It kills processes, so the kernel closes their sockets and
  Postgres learns at once. A black-holed connection was only imitated by a sleeping worker.
- **Postgres failing in any way other than process death**: power loss (`kill -9` keeps
  the OS cache, so durability of fsync is not tested), full disk, failover with a lagging
  replica (where an acknowledged commit can be lost), long vacuums.
- **Real clock skew.** All containers share one kernel clock; skew was only simulated.
- **Liveness and latency.** Invariants are checked on the final state. A job held hostage
  for a minute, or a cancel that took ten seconds, passes. The hostage problem was found
  by reasoning and a targeted test, not by the harness.
- **What clients see in between**: e.g. a status that goes backwards between two GETs.
- **Windows nobody thought of.** Crash points cover three named places. Random kills
  cover the rest at about 2% per kill.
- **Scale**: 10,000 rows, not millions (index bloat, vacuum, planner choices for filtered
  lists); jobs of milliseconds, not hours; 8 workers, not hundreds.
- **External effects.** The "side effect" is a row in the same database, in the same
  transaction. That is the easy case and the only one tested.
- **Replays.** A seed fixes the workload, not the interleaving; a failing run cannot be reproduced exactly.
- **Its own bugs.** Four were found (above). There is no reason to think that was all of them.

## Load: 100 requests per second

Open-loop test from inside the Docker network (`harness/loadtest.py`: 40% create, 40% get,
10% list, 10% cancel; latency measured from the scheduled send time), one API process, 4 workers:

| load | achieved | p50 | p95 | p99 | max | errors |
|---|---|---|---|---|---|---|
| 100 rps, first load after 17 h idle | 100 rps | 5.3 ms | 13.3 ms | **1,106 ms** | 1,438 ms | 0 |
| 100 rps, warm | 100 rps | 6.9 ms | 14.4 ms | 25.1 ms | 114 ms | 0 |
| 100 rps, right after an API restart | 100 rps | 7.2 ms | 11.1 ms | 18.2 ms | 201 ms | 0 |
| 300 rps | 300 rps | 25.7 ms | 154.8 ms | 244.8 ms | 327 ms | 0 |
| 1,000 rps | **322 rps** | 21 s | 40 s | 42 s | 42 s | 0 |

- At 100 rps the API is comfortable in two runs of three. **The first run's p99 of 1.1 s
  is not explained.** It did not reproduce warm or after an API restart, and no
  checkpoint was running. It was the first load after the stack had sat idle for 17 hours
  on a laptop that had slept; a VM or disk waking up is a guess, not a finding.
- One API process saturates at about 320 rps: roughly 3× headroom. Beyond that nothing
  is shed — requests queue and latency grows without bound.
- Under chaos at 100 new jobs/s (`20261116-161953.log`, API not killed) the host saw
  p50 11.8 ms and p99 83 ms. With the API killed every ~20 s (`20261116-170118-rps100.log`)
  each kill is 2–3 s of errors and throughput fell to 63 jobs/s.
- 100 new jobs/s is more than these 8 workers finish under chaos (about 28–36 jobs/s),
  so the backlog grew: median wait from create to success was 62–116 s. That is
  capacity, not correctness, but nothing limits the backlog or tells anyone about it.

## Verdict

**DON'T SHIP yet.** Not because the queue logic failed — it did not, in any run — but
because the evidence above is about one thing (state stays correct when processes die)
and production fails in other ways too. In the order I would fix them:

1. **One API process.** Every crash or deploy is 2–3 s of errors, measured. The API is
   stateless and every request is retry-safe, so two instances behind a load balancer
   with a readiness check is cheap and removes this.
2. **Nothing watches the queue.** No metric or alert for queue depth, age of the oldest
   queued job, lease expirations, refused writes or 503s. A queue that stops draining is
   the most likely real incident and today nobody would know. The backlog is unbounded.
3. **One Postgres, and no partition test.** Killed 27 times here with nothing lost, but
   each restart is a full outage (p99 4.6–5.5 s, 503s), there is no replica or backup,
   and neither failover nor a network partition was tested.
4. **The guarantee ends at the database.** A real training job writes checkpoints and
   metrics elsewhere. Those writes need to be idempotent per job id or carry the fencing
   token; cancel has to actually stop the work; heartbeats have to come from a separate
   thread. None of that exists here.
5. **The unexplained 1.1 s p99.** Reproduce it or explain it before promising a latency target.

With 1 and 2 done I would ship it for internal use where a short outage is acceptable;
3 and 4 come before it becomes the system of record for expensive jobs.

## What surprised me, and what still concerns me

The hardest part wasn't building the queue—it was proving it works. My first chaos test passed even after 40 workers died and zero jobs recovered, simply because it wasn't testing anything. Now, I only trust a green test if I can see the failure actually happened.

I was also surprised by how smart and wrong the AI could be at once. It nailed fencing and leases, yet wrote a test client that measured itself instead of the server and misdiagnosed a latency spike. Checking its claims against real numbers was the best thing I did.

A few things still worry me: an unexplained 1.1s latency spike, no real network partition tests, and the fact that "exactly-once" only holds because the result lives in the same database.

## AI tools

The whole submission was built in Claude Code (Anthropic's Claude; the session switched
models once), with the ECC plugin installed. To be plain about it: the code, the tests,
the harness, the experiments and the first draft of this document were written by the
AI agent, in a session I directed.

Suggestions from the AI that turned out to be wrong, and how that was found:

- **`httpx.AsyncClient` for the load generator.** ~40 requests/s on this host; found by measuring the API from inside the container (~350/s) and from plain threads (~330/s).
- **The first chaos loop.** Half the kill rate the task asks for, and a canceller that suppressed the recovery path. Found from the run statistics, not from a failing check.
- **"Cold start" as the cause of the 1.1 s p99.** Refuted by restarting the API and re-running; the cause is still unknown.
- **Treating a failed `docker kill` as "nothing happened".** Left a run with 7 workers.
- **An artificial 50 ms sleep** in the first `BUG_SPLIT_TX` control to make it easy to catch. Removed; the kill-only experiment measures the real window instead.
- The ECC plugin's "GateGuard" hook blocks the first write to every file until the agent recites a list of facts. It caught nothing in this session and only slowed the work down.

**What I verified, changed or redid myself:** *(to be filled in by me, truthfully, after
re-running the tests and the harness and reading the code — this section must not be
left as written by the AI.)*
