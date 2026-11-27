"""Bounded retail pipeline with repeatable, atomic stage checkpoints."""
import json
import logging
import os
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable

LOGGER = logging.getLogger(__name__)
REQUIRED = {"order_id", "customer_id", "product_id", "amount_cents"}
MAX_ROWS = 100_000


class SchemaError(ValueError):
    """A source schema requires operator review."""


def synthetic_orders(count: int) -> list[dict]:
    if not 1 <= count <= MAX_ROWS:
        raise ValueError(f"Expected 1 to {MAX_ROWS} synthetic orders")
    return [
        dict(order_id=i, customer_id=1 + i % 30, product_id=1 + i % 12,
             amount_cents=100 + i % 17 * 25)
        for i in range(1, count + 1)
    ]


def load_source(source: Callable[[], list[dict]], attempts: int = 3,
                sleep: Callable[[float], None] = time.sleep) -> tuple[list[dict], list[dict]]:
    if attempts < 1:
        raise ValueError("Expected at least one attempt")
    events = []
    for attempt in range(1, attempts + 1):
        try:
            return source(), events
        except (TimeoutError, ConnectionError):
            if attempt == attempts:
                raise
            delay = 0.1 * 2 ** (attempt - 1)
            events.append({"category": "source_connectivity", "attempt": attempt, "delay_seconds": delay})
            LOGGER.warning("source_retry attempt=%s delay=%s", attempt, delay)
            sleep(delay)
    raise AssertionError("unreachable")


def classify_schema(columns: set[str], allow_additions: bool) -> list[str]:
    missing = REQUIRED - columns
    if missing:
        raise SchemaError(f"Required fields removed or renamed: {', '.join(sorted(missing))}")
    added = sorted(columns - REQUIRED)
    if added and not allow_additions:
        raise SchemaError(f"New columns require review: {', '.join(added)}")
    return added


def silver(rows: list[dict]) -> dict:
    accepted, quarantine, seen = [], [], set()
    for row in rows:
        invalid = sorted(field for field in REQUIRED
                         if type(row.get(field)) is not int
                         or row[field] < (0 if field == "amount_cents" else 1))
        if invalid:
            quarantine.append({"original": row, "reason": "invalid fields", "fields": invalid})
        elif row["order_id"] in seen:
            quarantine.append({"original": row, "reason": "duplicate order_id", "fields": ["order_id"]})
        else:
            seen.add(row["order_id"])
            accepted.append({field: row[field] for field in sorted(REQUIRED)})
    return {"accepted": accepted, "quarantine": quarantine}


def gold(rows: list[dict]) -> list[dict]:
    counts = defaultdict(int)
    amounts = defaultdict(int)
    for row in rows:
        product_id = row["product_id"]
        counts[product_id] += 1
        amounts[product_id] += row["amount_cents"]
    return [
        {"product_id": product_id, "orders": counts[product_id], "revenue_cents": amounts[product_id]}
        for product_id in sorted(counts)
    ]


def checkpoint(directory: Path, stage: str, compute: Callable, resumed: list[str]):
    destination = directory / f"{stage}.json"
    if destination.exists():
        resumed.append(stage)
        return json.loads(destination.read_text(encoding="utf-8"))
    result = compute()
    with tempfile.NamedTemporaryFile(mode="w", dir=directory, encoding="utf-8", delete=False) as file:
        temporary = file.name
        try:
            json.dump(result, file, allow_nan=False)
            file.close()
            os.replace(temporary, destination)
        finally:
            Path(temporary).unlink(missing_ok=True)
    return result


def run(directory: Path, source: Callable[[], list[dict]], *,
        allow_additions: bool = True, fail_gold: bool = False) -> dict:
    """Use one private directory per run. A replay uses the original Bronze snapshot."""
    directory.mkdir(parents=True, exist_ok=True)
    resumed = []

    def ingest():
        rows, retries = load_source(source)
        if not 1 <= len(rows) <= MAX_ROWS:
            raise ValueError("Source batch is outside the permitted row range")
        columns = sorted(set().union(*(row.keys() for row in rows)))
        return {"rows": rows, "columns": columns, "ingested_epoch": time.time(), "retries": retries}

    bronze = checkpoint(directory, "bronze", ingest, resumed)
    added = classify_schema(set(bronze["columns"]), allow_additions)
    cleaned = checkpoint(directory, "silver", lambda: silver(bronze["rows"]), resumed)

    def transform():
        if fail_gold:
            raise RuntimeError("Injected Gold transformation failure")
        return gold(cleaned["accepted"])

    analytics = checkpoint(directory, "gold", transform, resumed)
    return {
        "input": len(bronze["rows"]),
        "valid": len(cleaned["accepted"]),
        "invalid": len(cleaned["quarantine"]),
        "quarantine": cleaned["quarantine"],
        "analytics": analytics,
        "compatible_additions": added,
        "retries": bronze["retries"],
        "restored_stages": resumed,
    }
