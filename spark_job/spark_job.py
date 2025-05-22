from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, from_json, when
from pyspark.sql.types import StructType, StringType

import config

# 1. Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaTicketStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# 2. Define ticket schema
ticket_schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("created_at", StringType()) \
    .add("demande", StringType()) \
    .add("type", StringType()) \
    .add("priorite", StringType())

# 3. Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.KAFKA_BROKER) \
    .option("subscribe", config.TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Extract and parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), ticket_schema).alias("data")) \
    .select("data.*")

# 5. Transform: parse timestamp & add fields
df = df_json.withColumn("created_at", col("created_at").cast("timestamp")) \
            .withColumn("year", year("created_at")) \
            .withColumn("month", month("created_at")) \
            .withColumn("day", dayofmonth("created_at")) \
            .withColumn("hour", hour("created_at")) \
            .withColumn("priorite_level",
                        when(col("priorite") == "low", 1)
                        .when(col("priorite") == "medium", 2)
                        .when(col("priorite") == "high", 3)
                        .when(col("priorite") == "urgent", 4)
                        .otherwise(0))

# 6. Write to Parquet (streaming sink)
df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "output/tickets_stream_parquet") \
    .option("checkpointLocation", "output/checkpoints/tickets") \
    .partitionBy("year", "month", "day") \
    .start() \
    .awaitTermination()
