"""Measure complete local lightweight runs including checkpoint writes."""
import json
import platform
import shutil
import time
import tracemalloc
from pathlib import Path
from uuid import uuid4

from pipelinex.engine import run, synthetic_orders


def measure(records):
    root = Path(f".bench-{uuid4().hex}")
    try:
        tracemalloc.start()
        start = time.perf_counter()
        result = run(root, lambda: synthetic_orders(records))
        seconds = time.perf_counter() - start
        _, peak = tracemalloc.get_traced_memory()
        assert result["valid"] + result["invalid"] == result["input"] == records
        assert sum(row["orders"] for row in result["analytics"]) == records
        return {"records": records, "seconds": round(seconds, 4), "peak_python_bytes": peak}
    finally:
        tracemalloc.stop()
        if root.exists():
            shutil.rmtree(root)


if __name__ == "__main__":
    print(json.dumps({"system": platform.platform(), "python": platform.python_version(),
                      "processor": platform.processor(),
                      "measurements": [measure(size) for size in (1000, 10000, 100000)]}, indent=2))
