from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour,
    from_json, when, count, countDistinct, avg,
    sum as spark_sum, first, last,
    to_timestamp, concat_ws, lit
)
from pyspark.sql.types import StructType, StringType
import config
import os

def main():
    print("=== Starting Spark Batch Job ===")
    spark = SparkSession.builder \
        .appName("KafkaTicketBatchStats") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    print(f"Kafka Broker: {config.KAFKA_BROKER}")
    print(f"Topic: {config.TOPIC}")

    # Define the schema for incoming tickets
    ticket_schema = StructType() \
        .add("ticket_id", StringType()) \
        .add("client_id", StringType()) \
        .add("created_at", StringType()) \
        .add("demande", StringType()) \
        .add("type", StringType()) \
        .add("priorite", StringType())

    try:
        # Read from Kafka
        df_raw = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BROKER) \
            .option("subscribe", config.TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Exit if no data
        if df_raw.rdd.isEmpty():
            print("❗ No messages in Kafka topic. Exiting.")
            spark.stop()
            return

        # Parse JSON and add timestamp fields
        df = (
            df_raw
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), ticket_schema).alias("data"))
            .select("data.*")
            .withColumn("created_at", col("created_at").cast("timestamp"))
            .filter(col("created_at").isNotNull())
            .withColumn("year", year("created_at"))
            .withColumn("month", month("created_at"))
            .withColumn("day", dayofmonth("created_at"))
            .withColumn("hour", hour("created_at"))
            .withColumn(
                "priorite_level",
                when(col("priorite") == "low", 1)
                .when(col("priorite") == "medium", 2)
                .when(col("priorite") == "high", 3)
                .when(col("priorite") == "urgent", 4)
                .otherwise(0)
            )
        )

        # Compute hourly statistics
        hourly_stats = (
            df.groupBy("year", "month", "day", "hour")
            .agg(
                count("ticket_id").alias("total_tickets"),
                spark_sum(when(col("priorite") == "low", 1).otherwise(0)).alias("low_count"),
                spark_sum(when(col("priorite") == "medium", 1).otherwise(0)).alias("medium_count"),
                spark_sum(when(col("priorite") == "high", 1).otherwise(0)).alias("high_count"),
                spark_sum(when(col("priorite") == "urgent", 1).otherwise(0)).alias("urgent_count"),
                avg("priorite_level").alias("avg_priority"),
                countDistinct("client_id").alias("unique_clients"),
                first("created_at").alias("window_start"),
                last("created_at").alias("window_end")
            )
            .withColumn(
                "stats_timestamp",
                to_timestamp(
                    concat_ws(" ",
                        concat_ws("-", col("year"), col("month"), col("day")),
                        concat_ws(":", col("hour"), lit("00"), lit("00"))
                    ),
                    "yyyy-M-d H:mm:ss"
                )
            )
            .withColumn("low_pct", (col("low_count") / col("total_tickets") * 100).cast("decimal(5,2)"))
            .withColumn("medium_pct", (col("medium_count") / col("total_tickets") * 100).cast("decimal(5,2)"))
            .withColumn("high_pct", (col("high_count") / col("total_tickets") * 100).cast("decimal(5,2)"))
            .withColumn("urgent_pct", (col("urgent_count") / col("total_tickets") * 100).cast("decimal(5,2)"))
        )

        # Ensure output directory
        out_dir = "/app/output"
        os.makedirs(out_dir, exist_ok=True)

        # Write Parquet and CSV
        parquet_path = f"{out_dir}/tickets_stats.parquet"
        hourly_stats.coalesce(1).write.mode("overwrite").parquet(parquet_path)
        print(f"✅ Parquet written to {parquet_path}")

        csv_path = f"{out_dir}/tickets_stats.csv"
        hourly_stats.coalesce(1).write.mode("overwrite").option("header","true").csv(csv_path)
        print(f"✅ CSV written to {csv_path}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
