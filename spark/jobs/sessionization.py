import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)
from pyspark.sql.functions import (
    col, from_json, to_timestamp, session_window, min as min_, max as max_,
    count, approx_count_distinct, sum as sum_, when, lit, concat_ws, sha2,
    date_format, coalesce
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events_raw")

SESSION_GAP = os.getenv("SESSION_GAP", "30 minutes")          # inactivity gap
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")  # allowed lateness

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/chk/sessionization_v1")


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def event_schema() -> StructType:
    # event_properties is a flexible map; Spark will keep unknown keys.
    return StructType([
        StructField("schema_version", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_time", StringType(), True),   # parse to Timestamp
        StructField("ingest_time", StringType(), True),  # parse to Timestamp
        StructField("user_id", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("app", StructType([
            StructField("app_name", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("environment", StringType(), True),
        ]), True),
        StructField("event_properties", MapType(StringType(), StringType(), True), True),
    ])


def main() -> None:
    spark = build_spark("sessionization_v1")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("value_json"),
            col("timestamp").alias("kafka_ingest_ts")
        )
        .select(from_json(col("value_json"), event_schema()).alias("e"))
        .select("e.*")
        # event_time in producer is ISO8601 (e.g. 2026-01-26T10:15:42.123456+00:00)
        .withColumn("event_time_ts", to_timestamp(col("event_time")))
        .withColumn("ingest_time_ts", to_timestamp(col("ingest_time")))
        .filter(col("user_id").isNotNull() & col("event_id").isNotNull() & col("event_name").isNotNull())
        .filter(col("event_time_ts").isNotNull())
    )

    # Dedup + lateness handling
    events = (
        parsed
        .withWatermark("event_time_ts", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )

    # Sessionize per user_id using event-time
    sessionized = (
        events
        .groupBy(
            col("user_id"),
            session_window(col("event_time_ts"), SESSION_GAP).alias("sw")
        )
        .agg(
            min_(col("event_time_ts")).alias("session_start_time"),
            max_(col("event_time_ts")).alias("session_end_time"),
            count(lit(1)).alias("events_count"),
            approx_count_distinct(col("event_name")).alias("unique_event_names_count"),
            sum_(when(col("event_name").isin("error_occurred", "api_error"), lit(1)).otherwise(lit(0))).alias("error_events"),
            # Dimensions: take any non-null value seen in the session
            min_(col("platform")).alias("platform"),
            min_(col("app.app_version")).alias("app_version"),
            min_(col("app.environment")).alias("environment"),
        )
        .withColumn("had_error", when(col("error_events") > 0, lit(1)).otherwise(lit(0)))
    )

    # Stable-ish session_id derived from user + session window boundaries
    out = (
        sessionized
        .withColumn(
            "session_id",
            sha2(
                concat_ws(
                    "|",
                    col("user_id"),
                    date_format(col("session_start_time"), "yyyy-MM-dd HH:mm:ss.SSS"),
                    date_format(col("session_end_time"), "yyyy-MM-dd HH:mm:ss.SSS")
                ),
                256
            )
        )
        .select(
            "session_id",
            "user_id",
            "session_start_time",
            "session_end_time",
            "events_count",
            "unique_event_names_count",
            "had_error",
            coalesce(col("platform"), lit("unknown")).alias("platform"),
            coalesce(col("app_version"), lit("unknown")).alias("app_version"),
            coalesce(col("environment"), lit("unknown")).alias("environment")
        )
    )

    query = (
        out.writeStream
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("complete")
        .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()
