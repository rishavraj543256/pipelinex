"""Run cold-batch Delta ingestion and replay measurements in local Spark."""
import argparse
import json
import platform
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from pipelinex.engine import synthetic_orders
from pipelinex.spark_engine import execute, session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sizes", nargs="*", type=int, default=[1000, 10000, 100000])
    args = parser.parse_args()
    spark = session()
    results = []
    try:
        with TemporaryDirectory() as folder:
            root = Path(folder)
            lake = (root / "lake").as_uri()
            for index, size in enumerate(args.sizes):
                source = root / f"orders-{index}.json"
                with source.open("w", encoding="utf-8") as output:
                    for row in synthetic_orders(size):
                        output.write(json.dumps(row) + "\n")
                batch = f"bench-{index}"
                start = time.perf_counter()
                first = execute(spark, source.as_uri(), lake, batch)
                initial_seconds = time.perf_counter() - start
                source.unlink()
                start = time.perf_counter()
                replay = execute(spark, source.as_uri(), lake, batch)
                replay_seconds = time.perf_counter() - start
                assert first == replay == {"input": size, "valid": size, "quarantined": 0, "gold_orders": size}
                results.append({"orders": size, "initial_seconds": initial_seconds,
                                "replay_seconds": replay_seconds})
        print(json.dumps({"platform": platform.platform(), "spark": spark.version,
                          "master": spark.sparkContext.master, "results": results}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
