"""HTTP API: create (idempotent), get, list (keyset pagination), cancel."""
import os
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional
from uuid import UUID

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from . import queue as q

Status = Literal["queued", "running", "succeeded", "failed", "cancelled"]
pool: ConnectionPool


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = ConnectionPool(q.DATABASE_URL, min_size=2, max_size=int(os.environ.get("DB_POOL_MAX", "20")),
                          timeout=float(os.environ.get("DB_POOL_TIMEOUT", "5")),   # fail fast, do not queue forever
                          kwargs=q.CONN_KWARGS, configure=q.configure, open=False)
    pool.open(wait=True, timeout=60)
    yield
    pool.close()


app = FastAPI(title="training-job queue", lifespan=lifespan)


@app.exception_handler(psycopg.OperationalError)
async def temporarily_unavailable(request, exc):
    """Database down, pool exhausted, or a row lock not granted within lock_timeout.
    Every endpoint is safe to retry (creates carry an idempotency key), so say so
    instead of hanging or answering 500."""
    return JSONResponse(status_code=503, headers={"Retry-After": "1"},
                        content={"detail": f"temporarily unavailable ({type(exc).__name__}), retry"})


class CreateJob(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)


def public(job):
    return {k: v for k, v in job.items() if k not in ("request_hash", "seq")}


def respond(code, job):
    return JSONResponse(status_code=code, content=jsonable_encoder(public(job)))


@app.get("/healthz")
def healthz():
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"ok": True}


@app.post("/jobs")
def create_job(body: CreateJob, idempotency_key: str = Header(min_length=1, max_length=200)):
    """201 = created, 200 = same key and same body (replay), 409 = same key, different body."""
    with pool.connection() as conn:
        job, outcome = q.create_job(conn, idempotency_key, body.payload)
    if outcome == "conflict":
        raise HTTPException(409, "Idempotency-Key was already used with a different request body")
    return respond(201 if outcome == "created" else 200, job)


@app.get("/jobs/{job_id}")
def get_job(job_id: UUID):
    with pool.connection() as conn:
        job = q.get_job(conn, job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return respond(200, job)


@app.get("/jobs")
def list_jobs(status: Optional[Status] = None, cursor: int = Query(0, ge=0),
              limit: int = Query(50, ge=1, le=500)):
    with pool.connection() as conn:
        rows = q.list_jobs(conn, status, cursor, limit)
    return {"items": jsonable_encoder([public(r) for r in rows]),
            "next_cursor": rows[-1]["seq"] if len(rows) == limit else None}


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: UUID):
    """200 = job is cancelled (now or already). 409 = it had already succeeded/failed."""
    with pool.connection() as conn:
        job, outcome = q.cancel_job(conn, job_id)
    if outcome == "not_found":
        raise HTTPException(404, "job not found")
    return respond(409 if outcome == "terminal" else 200, job)
