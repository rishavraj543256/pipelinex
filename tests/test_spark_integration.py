import json
import os
import unittest

from pipelinex.engine import synthetic_orders
from tests.test_engine import workspace_temp


@unittest.skipUnless(os.environ.get("PIPELINEX_TEST_SPARK") == "1", "Requires Java and Delta Maven packages")
class SparkIntegration(unittest.TestCase):
    def test_delta_recovery_and_reconciliation(self):
        from pipelinex.spark_engine import execute, session

        spark = session()
        try:
            with workspace_temp() as root:
                source = root / "orders.json"
                rows = synthetic_orders(24)
                rows.append(rows[0])
                source.write_text("\n".join(map(json.dumps, rows)), encoding="utf-8")
                source_uri = source.as_uri()
                lake = (root / "lake").as_uri()
                with self.assertRaisesRegex(RuntimeError, "Injected Gold"):
                    execute(spark, source_uri, lake, "batch1", fail_gold=True)
                source.unlink()
                result = execute(spark, source_uri, lake, "batch1")
                self.assertEqual(result, {"input": 25, "valid": 24, "quarantined": 1, "gold_orders": 24})
        finally:
            spark.stop()
