"""Single-writer Delta ingestion ledger with forward and historical windows."""
import re

from pipelinex.spark_engine import execute


def ingest(spark, source_uri: str, root: str, batch: str, upper: int, *,
           backfill_lower: int | None = None, fail_gold: bool = False):
    """Apply (lower, upper] per source namespace. Retain raw snapshots for replay.

    Serialize calls for a given root. Upstream must finalize data through upper
    before a forward batch starts. Late arrivals require explicit backfill.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegralType

    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", batch):
        raise ValueError("Invalid batch identifier")
    if upper < 0 or (backfill_lower is not None and not -1 <= backfill_lower < upper):
        raise ValueError("Invalid watermark window")
    root = root.rstrip("/")
    ledger_path = f"{root}/_ingestion_ledger"
    ledger_exists = DeltaTable.isDeltaTable(spark, ledger_path)
    schema = "batch string, lower_bound long, upper_bound long, backfill boolean, completed boolean"
    ledger = (spark.read.format("delta").load(ledger_path) if ledger_exists
              else spark.createDataFrame([], schema))
    prior = ledger.filter(F.col("batch") == batch).first()
    backfill = backfill_lower is not None
    if prior:
        if (prior.upper_bound != upper or prior.backfill != backfill
                or (backfill and prior.lower_bound != backfill_lower)):
            raise ValueError("Batch identifier already reserved for a different window")
        lower = prior.lower_bound
    else:
        if ledger.filter(~F.col("completed")).take(1):
            raise RuntimeError("Resume the pending batch before starting another")
        high = ledger.filter(~F.col("backfill")).agg(F.max("upper_bound")).first()[0]
        lower = backfill_lower if backfill else (-1 if high is None else high)
        if upper <= lower:
            raise ValueError("Forward watermark must advance")
        reservation = spark.createDataFrame([(batch, lower, upper, backfill, False)], schema)
        reservation.write.format("delta").mode("append").save(ledger_path)

    frame = None
    if not DeltaTable.isDeltaTable(spark, f"{root}/{batch}/bronze"):
        raw = spark.read.option("mode", "FAILFAST").json(source_uri)
        if ("watermark" not in raw.columns or
                not isinstance(raw.schema["watermark"].dataType, IntegralType) or
                raw.filter(F.col("watermark").isNull()).take(1)):
            raise ValueError("Source requires a non-null integral watermark")
        frame = raw.filter(F.col("watermark").between(lower + 1, upper))
    result = execute(spark, source_uri, root, batch, frame=frame, fail_gold=fail_gold)
    DeltaTable.forPath(spark, ledger_path).update(
        F.col("batch") == batch, {"completed": F.lit(True)}
    )
    return {**result, "lower_exclusive": lower, "upper_inclusive": upper, "backfill": backfill}
