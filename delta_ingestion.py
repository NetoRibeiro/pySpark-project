"""
Reusable Delta Lake ingestion helpers for platform-partitioned ID discovery.

For each entity (User, Profile, Series, Title), the helper isolates brand-new IDs
per incoming batch, lands them into a Delta "increments" path (partitioned by
platform), and appends them into the master history for future deduplication.
"""
from typing import Tuple

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

# Base folders for Delta Lake storage. These can be redirected to cloud or local
# storage by changing the root prefixes.
MASTER_HISTORY_ROOT = "delta/master_history"
DELTA_INCREMENTS_ROOT = "delta/delta_increments"


def _normalize_source(source_df: DataFrame, id_col: str) -> DataFrame:
    """Selects the required columns and stamps ingestion time.

    The result schema is always (id, platform, ingest_timestamp) to keep master
    and delta tables consistent across all entities.
    """

    return (
        source_df.select(
            F.col(id_col).alias("id"),
            F.col("platform"),
            F.current_timestamp().alias("ingest_timestamp"),
        )
        .dropna(subset=["id", "platform"])
        .dropDuplicates(["id", "platform"])
    )


def _master_table(spark, master_path: str, schema_df: DataFrame) -> DataFrame:
    """Loads an existing master Delta table or returns an empty DataFrame."""

    if DeltaTable.isDeltaTable(spark, master_path):
        return spark.read.format("delta").load(master_path).select("id").distinct()
    return spark.createDataFrame([], schema_df.select("id").schema)


def _write_delta(df: DataFrame, path: str) -> None:
    """Writes Delta data partitioned by platform."""

    df.write.format("delta").mode("append").partitionBy("platform").save(path)


def process_entity_delta(source_df: DataFrame, entity_name: str, id_col: str) -> Tuple[DataFrame, str, str]:
    """Computes and writes the Delta increment for a single entity.

    Args:
        source_df: Incoming batch DataFrame containing the entity ID and a
            ``platform`` column.
        entity_name: Entity label (e.g., ``"user"`` or ``"series"``) to build
            folder names.
        id_col: Column name of the entity identifier inside ``source_df``.

    Returns:
        A tuple of (delta_df, delta_path, master_path) where ``delta_df`` is the
        newly discovered set of rows.
    """

    spark = source_df.sparkSession
    normalized = _normalize_source(source_df, id_col)

    master_path = f"{MASTER_HISTORY_ROOT}/{entity_name}"
    delta_path = f"{DELTA_INCREMENTS_ROOT}/{entity_name}"

    master_df = _master_table(spark, master_path, normalized)
    delta_df = normalized.join(master_df, on="id", how="left_anti")

    if delta_df.rdd.isEmpty():
        return delta_df, delta_path, master_path

    _write_delta(delta_df, delta_path)
    _write_delta(delta_df, master_path)
    return delta_df, delta_path, master_path


if __name__ == "__main__":
    # Example usage with one incoming batch that has all entity IDs plus platform.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("DeltaEntityIngestion").getOrCreate()

    # Replace this with your real ingestion source (Parquet, CSV, etc.). The
    # DataFrame must contain the ID columns and a ``platform`` column.
    batch_df = spark.createDataFrame(
        [
            ("u1", "p1", "s1", "t1", "netflix"),
            ("u2", "p2", "s2", "t2", "netflix"),
            ("u1", "p1", "s3", "t3", "hulu"),
        ],
        ["user_id", "profile_id", "series_id", "title_id", "platform"],
    )

    process_entity_delta(batch_df.select("user_id", "platform"), "user", "user_id")
    process_entity_delta(batch_df.select("profile_id", "platform"), "profile", "profile_id")
    process_entity_delta(batch_df.select("series_id", "platform"), "series", "series_id")
    process_entity_delta(batch_df.select("title_id", "platform"), "title", "title_id")

    spark.stop()
