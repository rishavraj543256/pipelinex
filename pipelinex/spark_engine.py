"""Local Spark and Delta execution for immutable retail ingestion batches."""
import argparse
import json
import re
from pathlib import Path

from pipelinex.engine import REQUIRED, classify_schema


def session():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("PipelineX").master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def execute(spark, source: str, root: str, batch: str, *, fail_gold: bool = False,
            frame=None):
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegralType

    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", batch):
        raise ValueError("Invalid batch identifier")
    root = root.rstrip("/")

    def save_once(layer, dataframe, partition=None):
        path = f"{root}/{batch}/{layer}"
        if not DeltaTable.isDeltaTable(spark, path):
            writer = dataframe.write.format("delta").mode("errorifexists")
            if partition:
                writer = writer.partitionBy(partition)
            writer.save(path)
        return spark.read.format("delta").load(path)

    bronze_path = f"{root}/{batch}/bronze"
    if DeltaTable.isDeltaTable(spark, bronze_path):
        bronze = spark.read.format("delta").load(bronze_path)
    else:
        raw = frame if frame is not None else spark.read.option("mode", "FAILFAST").json(source)
        classify_schema(set(raw.columns), allow_additions=True)
        if {"_batch", "_ingested_at", "_raw"} & set(raw.columns):
            raise ValueError("Source uses a reserved metadata column")
        bronze = save_once(
            "bronze",
            raw.withColumn("_raw", F.to_json(F.struct(*raw.columns), {"ignoreNullFields": "false"}))
            .withColumn("_batch", F.lit(batch)).withColumn("_ingested_at", F.current_timestamp()),
        )

    fields = sorted(REQUIRED)
    typed = bronze
    for field in fields:
        schema_type = bronze.schema[field].dataType
        value = F.col(field).cast("long") if isinstance(schema_type, IntegralType) else F.lit(None).cast("long")
        typed = typed.withColumn(f"_valid_{field}", value)
    invalid_fields = F.concat_ws(",", *[
        F.when(F.col(f"_valid_{field}").isNull() | (F.col(f"_valid_{field}") < (0 if field == "amount_cents" else 1)), field)
        for field in fields
    ])
    checked = typed.withColumn("_invalid", invalid_fields)
    valid = checked.filter(F.col("_invalid") == "")
    rank = Window.partitionBy("_valid_order_id").orderBy("_raw")
    ranked = valid.withColumn("_rank", F.row_number().over(rank))
    duplicates = ranked.filter("_rank > 1").select("_raw", F.lit("duplicate order_id").alias("reason"))
    invalid = checked.filter(F.col("_invalid") != "").select("_raw", F.concat(F.lit("invalid fields: "), "_invalid").alias("reason"))
    quarantine = save_once("quarantine", invalid.unionByName(duplicates))
    silver = save_once("silver", ranked.filter("_rank = 1").select(*[
        F.col(f"_valid_{field}").alias(field) for field in fields
    ]), "product_id")
    if fail_gold:
        raise RuntimeError("Injected Gold transformation failure")

    silver.createOrReplaceTempView("pipelinex_batch_silver")
    totals = spark.sql("""
        SELECT product_id, count(*) AS orders, sum(amount_cents) AS revenue_cents
        FROM pipelinex_batch_silver GROUP BY product_id
    """)
    gold = save_once("gold", totals)
    return {"input": bronze.count(), "valid": silver.count(), "quarantined": quarantine.count(),
            "gold_orders": gold.agg(F.sum("orders")).first()[0] or 0}


def merge_current(spark, incoming, path: str):
    """SCD1 upsert. The caller must provide one row per key and a monotonic integer watermark."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        incoming.write.format("delta").mode("errorifexists").save(path)
        return
    table = DeltaTable.forPath(spark, path)
    (table.alias("target").merge(incoming.alias("source"), "target.order_id = source.order_id")
     .whenMatchedUpdateAll(condition="source.watermark > target.watermark")
     .whenNotMatchedInsertAll().execute())


def merge_history(spark, incoming, path: str):
    """Atomically expire and insert versions for one ordered change per key."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    fields = sorted(REQUIRED - {"order_id"})
    candidates = incoming.select("order_id", "watermark", *fields)
    if candidates.filter("order_id IS NULL OR watermark IS NULL").take(1):
        raise ValueError("Keys and watermarks must be non-null")
    if candidates.groupBy("order_id").count().filter("count > 1").take(1):
        raise ValueError("Apply at most one change per key per batch")
    candidates = candidates.withColumn(
        "fingerprint", F.sha2(F.to_json(F.struct(*fields), {"ignoreNullFields": "false"}), 256)
    )
    versions = (candidates.withColumn("valid_from", F.col("watermark"))
                .withColumn("valid_to", F.lit(None).cast("long"))
                .withColumn("is_current", F.lit(True)).drop("watermark"))
    if not DeltaTable.isDeltaTable(spark, path):
        versions.write.format("delta").mode("errorifexists").save(path)
        return

    table = DeltaTable.forPath(spark, path)
    active = table.toDF().filter("is_current").select(
        "order_id", F.col("valid_from").alias("active_from"),
        F.col("fingerprint").alias("active_fingerprint"),
    )
    changes = versions.join(active, "order_id", "left").filter(
        "active_from IS NULL OR (valid_from > active_from AND fingerprint <> active_fingerprint)"
    )
    inserts = changes.select(*versions.columns).withColumn("merge_key", F.lit(None).cast("long"))
    expirations = changes.filter("active_from IS NOT NULL").select(*versions.columns).withColumn(
        "merge_key", F.col("order_id")
    )
    (table.alias("target").merge(inserts.unionByName(expirations).alias("source"),
                                 "target.order_id = source.merge_key AND target.is_current")
     .whenMatchedUpdate(set={"valid_to": "source.valid_from", "is_current": "false"})
     .whenNotMatchedInsert(condition="source.merge_key IS NULL",
                           values={column: f"source.{column}" for column in versions.columns})
     .execute())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("lakehouse", type=Path)
    parser.add_argument("batch")
    args = parser.parse_args()
    spark = session()
    try:
        print(json.dumps(execute(spark, args.source.resolve().as_uri(),
                                 args.lakehouse.resolve().as_uri(), args.batch)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
