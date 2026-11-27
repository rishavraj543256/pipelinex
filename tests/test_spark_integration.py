import json
import os
import unittest

from pipelinex.engine import synthetic_orders
from tests.test_engine import workspace_temp


@unittest.skipUnless(os.environ.get("PIPELINEX_TEST_SPARK") == "1", "Requires Java and Delta Maven packages")
class SparkIntegration(unittest.TestCase):
    def test_history_merge_replay(self):
        from pipelinex.spark_engine import merge_history, session

        spark = session()
        try:
            with workspace_temp() as root:
                history = (root / "history").as_uri()
                columns = ["order_id", "customer_id", "product_id", "amount_cents", "watermark"]
                initial = spark.createDataFrame([(1, 2, 3, 100, 10)], columns)
                update = spark.createDataFrame([(1, 2, 3, 125, 20), (2, 2, 3, 75, 20)], columns)
                merge_history(spark, initial, history)
                merge_history(spark, update, history)
                merge_history(spark, update, history)
                rows = spark.read.format("delta").load(history).orderBy("order_id", "valid_from").collect()
                self.assertEqual([(r.order_id, r.valid_from, r.valid_to, r.is_current) for r in rows],
                                 [(1, 10, 20, False), (1, 20, None, True), (2, 20, None, True)])
        finally:
            spark.stop()

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
