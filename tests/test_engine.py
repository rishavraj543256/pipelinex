import shutil
import unittest
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from pipelinex.engine import SchemaError, load_source, run, synthetic_orders


@contextmanager
def workspace_temp():
    path = Path.cwd() / f".test-run-{uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class PipelineTests(unittest.TestCase):
    def test_counts_and_original_quarantine_values(self):
        rows = synthetic_orders(10)
        rows += [rows[0].copy(), {**rows[1], "amount_cents": "invalid"}]
        with workspace_temp() as directory:
            result = run(Path(directory), lambda: rows)
        self.assertEqual((result["input"], result["valid"], result["invalid"]), (12, 10, 2))
        self.assertEqual(result["quarantine"][1]["original"]["amount_cents"], "invalid")
        self.assertEqual(sum(row["orders"] for row in result["analytics"]), 10)
        self.assertEqual(sum(row["revenue_cents"] for row in result["analytics"]),
                         sum(row["amount_cents"] for row in rows[:10]))

    def test_incompatible_schema_stops_before_silver(self):
        with workspace_temp() as directory:
            root = Path(directory)
            with self.assertRaises(SchemaError):
                run(root, lambda: [{"order_id": 1}])
            self.assertTrue((root / "bronze.json").exists())
            self.assertFalse((root / "silver.json").exists())

    def test_added_column_policy(self):
        rows = [{**row, "region": "west"} for row in synthetic_orders(5)]
        with workspace_temp() as directory:
            root = Path(directory)
            with self.assertRaises(SchemaError):
                run(root, lambda: rows, allow_additions=False)
            self.assertEqual(run(root, lambda: rows)["compatible_additions"], ["region"])

    def test_transient_source_retry(self):
        calls = []
        delays = []

        def source():
            calls.append(1)
            if len(calls) < 3:
                raise TimeoutError()
            return synthetic_orders(1)

        rows, events = load_source(source, sleep=delays.append)
        self.assertEqual(len(rows), 1)
        self.assertEqual(delays, [0.1, 0.2])
        self.assertEqual(len(events), 2)

    def test_checkpoint_resume_does_not_reread_source(self):
        def unavailable():
            self.fail("Checkpoint replay called the source")

        with workspace_temp() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "Injected Gold"):
                run(root, lambda: synthetic_orders(10), fail_gold=True)
            self.assertFalse((root / "gold.json").exists())
            recovered = run(root, unavailable)
            repeated = run(root, unavailable)
            self.assertEqual(recovered["restored_stages"], ["bronze", "silver"])
            self.assertEqual(repeated["restored_stages"], ["bronze", "silver", "gold"])
            self.assertEqual(recovered["analytics"], repeated["analytics"])
