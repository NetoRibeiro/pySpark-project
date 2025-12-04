import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


TYPE_MAPPING = {
    "string": T.StringType(),
    "integer": T.IntegerType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "float": T.FloatType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
}


def parse_args():
    parser = argparse.ArgumentParser(description="Metadata-driven ETL job")
    parser.add_argument("--platform", required=True, help="Platform identifier (e.g., netflix)")
    parser.add_argument("--target_date", required=True, help="Target date in yyyy-mm-dd format")
    parser.add_argument("--weeks_back", type=int, default=4, help="Number of historical weeks to scan")
    return parser.parse_args()


def load_yaml(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def build_schema(platform_mapping: Dict) -> T.StructType:
    fields = []
    for spec in platform_mapping.get("columns", {}).values():
        name = spec.get("name")
        dtype = TYPE_MAPPING.get(spec.get("type", "string"), T.StringType())
        fields.append(T.StructField(name, dtype, True))
    return T.StructType(fields)


def generate_dates(target_date: str, weeks_back: int) -> List[str]:
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    return [(target - timedelta(days=7 * i)).isoformat() for i in range(1, weeks_back + 1)]


def build_paths(aws_config: Dict, platform: str, dates: List[str]) -> List[str]:
    bucket = aws_config.get("bucket")
    base_path = aws_config.get("base_path", "").strip("/")
    pattern = aws_config.get("patterns", {}).get(platform, "{date}.txt")
    base_prefix = f"s3a://{bucket}/" + (base_path + "/" if base_path else "")
    return [base_prefix + pattern.format(date=date) for date in dates]


def filter_existing_paths(spark: SparkSession, paths: List[str]) -> List[str]:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    existing = []
    for path in paths:
        if fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
            existing.append(path)
    return existing


def read_platform_data(spark: SparkSession, platform_mapping: Dict, paths: List[str]) -> Optional["DataFrame"]:
    if not paths:
        schema = build_schema(platform_mapping)
        return spark.createDataFrame([], schema)
    return spark.read.option("header", True).option("inferSchema", True).csv(paths)


def standardize_dataframe(df, platform_mapping: Dict):
    column_specs = platform_mapping.get("columns", {})
    if not column_specs:
        return df

    select_exprs = []
    for raw_col, spec in column_specs.items():
        target_name = spec.get("name")
        dtype = spec.get("type", "string").lower()
        col_expr = F.col(raw_col) if raw_col in df.columns else F.lit(None)
        if dtype == "date":
            col_expr = F.to_date(col_expr)
        elif dtype == "timestamp":
            col_expr = F.to_timestamp(col_expr)
        else:
            col_expr = col_expr.cast(TYPE_MAPPING.get(dtype, T.StringType()))
        select_exprs.append(col_expr.alias(target_name))
    return df.select(*select_exprs)


def build_bloom_filters(df_history, history_count: int, columns: List[str]):
    filters = {}
    expected_items = max(history_count, 1000)
    for column in columns:
        if column in df_history.columns:
            filters[column] = df_history.stat.bloomFilter(column, expected_items, 0.01)
    return filters


def compute_spike_summary(df_history, target_count: int, thresholds: Dict):
    z_threshold = thresholds.get("zscore_threshold", 3.0)
    date_col = "event_date" if "event_date" in df_history.columns else None
    if date_col:
        daily_counts = df_history.groupBy(date_col).count()
    else:
        daily_counts = df_history.withColumn("event_date", F.lit("history"))\
            .groupBy("event_date").count()
    stats = daily_counts.agg(F.mean("count").alias("mean"), F.stddev("count").alias("stddev")).collect()[0]
    mean_val = stats["mean"] or 0.0
    std_val = stats["stddev"] or 0.0
    z_score = (target_count - mean_val) / std_val if std_val > 0 else 0.0
    return {
        "target_count": target_count,
        "history_mean": mean_val,
        "history_stddev": std_val,
        "z_score": z_score,
        "threshold": z_threshold,
        "is_spike": z_score > z_threshold,
    }


def compute_trending(df_target, df_history, history_periods: int):
    if "series_title" not in df_target.columns:
        return []
    target_counts = df_target.groupBy("series_title").count().withColumnRenamed("count", "target_count")
    if history_periods and "series_title" in df_history.columns:
        history_counts = df_history.groupBy("series_title").count().withColumnRenamed("count", "history_total")
        history_avg = history_counts.withColumn("history_avg", F.col("history_total") / F.lit(float(history_periods)))
        trending = target_counts.join(history_avg, "series_title", "left")
    else:
        trending = target_counts.withColumn("history_avg", F.lit(0.0))
    trending = trending.withColumn(
        "growth_pct",
        F.when(F.col("history_avg") > 0, (F.col("target_count") - F.col("history_avg")) / F.col("history_avg") * 100)
        .otherwise(F.lit(None))
    )
    top = trending.orderBy(F.col("growth_pct").desc_nulls_last(), F.col("target_count").desc()).limit(10)
    return [
        {
            "series_title": row["series_title"],
            "target_count": row["target_count"],
            "history_avg": row["history_avg"],
            "growth_pct": row["growth_pct"],
        }
        for row in top.collect()
    ]


def compute_discovery(df_target, bloom_filters: Dict[str, object]):
    if not bloom_filters:
        return {
            "new_users": int(df_target.select("user_id").distinct().count()) if "user_id" in df_target.columns else 0,
            "new_series": int(df_target.select("series_title").distinct().count()) if "series_title" in df_target.columns else 0,
            "new_series_list": [row[0] for row in df_target.select("series_title").distinct().limit(10).collect()] if "series_title" in df_target.columns else [],
        }

    spark = df_target.sql_ctx.sparkSession
    broadcasts = {col: spark.sparkContext.broadcast(bf) for col, bf in bloom_filters.items()}

    @F.udf("boolean")
    def is_new_user(value):
        if value is None or "user_id" not in broadcasts:
            return False
        # Bloom filter: fast membership test, may accept ~1% false positives but avoids shuffle heavy joins.
        return not broadcasts["user_id"].value.mightContain(value)

    @F.udf("boolean")
    def is_new_series(value):
        if value is None or "series_title" not in broadcasts:
            return False
        return not broadcasts["series_title"].value.mightContain(value)

    new_users_df = df_target.filter(is_new_user(F.col("user_id"))) if "user_id" in df_target.columns else df_target.limit(0)
    new_series_df = df_target.filter(is_new_series(F.col("series_title"))) if "series_title" in df_target.columns else df_target.limit(0)

    return {
        "new_users": int(new_users_df.select("user_id").distinct().count()) if "user_id" in df_target.columns else 0,
        "new_series": int(new_series_df.select("series_title").distinct().count()) if "series_title" in df_target.columns else 0,
        "new_series_list": [row[0] for row in new_series_df.groupBy("series_title").count().orderBy(F.col("count").desc()).limit(10).collect()] if "series_title" in df_target.columns else [],
    }


def main():
    args = parse_args()
    aws_config = load_yaml("aws.yaml")
    platform_mappings = load_yaml("platforms.yaml")
    thresholds = load_yaml("thresholds.yaml")

    platform_mapping = platform_mappings.get(args.platform, {})

    spark = (
        SparkSession.builder.appName("MetadataDrivenETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    history_dates = generate_dates(args.target_date, args.weeks_back)
    target_paths = build_paths(aws_config, args.platform, [args.target_date])
    history_paths = build_paths(aws_config, args.platform, history_dates)

    existing_target_paths = filter_existing_paths(spark, target_paths)
    existing_history_paths = filter_existing_paths(spark, history_paths)

    df_target_raw = read_platform_data(spark, platform_mapping, existing_target_paths)
    df_history_raw = read_platform_data(spark, platform_mapping, existing_history_paths)

    df_target = standardize_dataframe(df_target_raw, platform_mapping)
    df_history = standardize_dataframe(df_history_raw, platform_mapping)

    df_target.cache()
    df_history.cache()

    target_count = df_target.count()
    history_count = df_history.count()
    history_periods = max(len(existing_history_paths), 1)

    bloom_filters = build_bloom_filters(df_history, history_count, ["user_id", "series_title"])

    spike_summary = compute_spike_summary(df_history, target_count, thresholds)
    trending = compute_trending(df_target, df_history, history_periods if history_count > 0 else 0)
    discovery = compute_discovery(df_target, bloom_filters if history_count > 0 else {})

    analytics = {
        "target_date": args.target_date,
        "platform": args.platform,
        "record_count": target_count,
        "history_record_count": history_count,
    }

    output = {
        "analytics": analytics,
        "spike_summary": spike_summary,
        "trending": trending,
        "discovery": discovery,
    }

    print(json.dumps(output, default=str))

    spark.stop()


if __name__ == "__main__":
    main()
