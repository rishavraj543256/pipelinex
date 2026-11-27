"""Bounded synthetic lab API. Session credentials are random and stored as digests."""
import hashlib
import os
import secrets
import time
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi import Depends
from pydantic import BaseModel, Field
from sqlalchemy import JSON, BigInteger, ForeignKey, String, create_engine, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from pipelinex.engine import SchemaError, run, synthetic_orders


class Base(DeclarativeBase):
    pass


class DemoSession(Base):
    __tablename__ = "demo_sessions"
    digest: Mapped[str] = mapped_column(String(64), primary_key=True)
    expires: Mapped[int] = mapped_column(BigInteger, index=True)
    remaining: Mapped[int]


class Execution(Base):
    __tablename__ = "demo_runs"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner: Mapped[str] = mapped_column(ForeignKey("demo_sessions.digest", ondelete="CASCADE"), index=True)
    created: Mapped[int] = mapped_column(BigInteger)
    details: Mapped[dict] = mapped_column(JSON)


class Scenario(BaseModel):
    records: int = Field(100, ge=1, le=500)
    failure: Literal["none", "duplicate", "invalid", "added_column", "removed_field",
                     "temporary_source", "gold"] = "none"


app = FastAPI(title="PipelineX Synthetic Lab")
bearer = HTTPBearer()


@lru_cache
def engine():
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


def database():
    with Session(engine()) as db:
        yield db


def digest(token: str):
    return hashlib.sha256(token.encode()).hexdigest()


def authenticated(credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    return digest(credentials.credentials)


@app.get("/ready")
def ready(db: Session = Depends(database)):
    db.execute(text("SELECT 1 FROM demo_sessions LIMIT 1"))
    return {"ready": True, "engine": "bounded-python"}


@app.post("/demo/sessions", status_code=201)
def create_session(db: Session = Depends(database)):
    token = secrets.token_urlsafe(32)
    db.add(DemoSession(digest=digest(token), expires=int(time.time()) + 3600, remaining=5))
    db.commit()
    return {"token": token, "expires_in": 3600}


def execute_scenario(body: Scenario):
    rows = synthetic_orders(body.records)
    if body.failure == "duplicate":
        rows.append(rows[0].copy())
    elif body.failure == "invalid":
        rows.append({**rows[0], "amount_cents": "invalid"})
    elif body.failure == "added_column":
        rows[0]["region"] = "west"
    elif body.failure == "removed_field":
        for row in rows:
            row.pop("amount_cents")
    attempts = 0

    def source():
        nonlocal attempts
        attempts += 1
        if attempts == 1 and body.failure == "temporary_source":
            raise TimeoutError("Injected source timeout")
        return rows

    with TemporaryDirectory() as directory:
        root = Path(directory)
        try:
            if body.failure == "gold":
                try:
                    run(root, source, fail_gold=True)
                except RuntimeError:
                    return {"status": "recovered", **run(root, source)}
            return {"status": "completed", **run(root, source)}
        except SchemaError as error:
            return {"status": "review_required", "category": "schema", "message": str(error)}


@app.post("/demo/runs", status_code=201)
def start_run(body: Scenario, owner: str = Depends(authenticated), db: Session = Depends(database)):
    session = db.scalar(select(DemoSession).where(DemoSession.digest == owner).with_for_update())
    if session is None or session.expires <= time.time():
        raise HTTPException(401, "Demo session expired or invalid")
    if session.remaining <= 0:
        raise HTTPException(429, "Demo execution quota reached")
    session.remaining -= 1
    result = execute_scenario(body)
    execution = Execution(id=secrets.token_hex(16), owner=owner, created=int(time.time()), details=result)
    db.add(execution)
    db.commit()
    return {"id": execution.id, **result}


@app.get("/demo/runs/{run_id}")
def get_run(run_id: str, owner: str = Depends(authenticated), db: Session = Depends(database)):
    execution = db.scalar(select(Execution).join(DemoSession).where(
        Execution.id == run_id, Execution.owner == owner, DemoSession.expires > int(time.time())
    ))
    if execution is None:
        raise HTTPException(404, "Execution not found")
    return {"id": execution.id, **execution.details}
