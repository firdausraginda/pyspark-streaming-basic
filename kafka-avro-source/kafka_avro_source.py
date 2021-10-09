from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Kafka Avro Source") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "earliest") \
        .load()

    avro_schema = open("./avro-schema/invoice_items", "r").read()

    value_df = kafka_source_df.select(
        from_avro(col("value"), avro_schema).alias("value"))

    rewards_df = value_df.filter("value.CustomerType == 'PRIME' ") \
        .groupBy("value.CustomerCardNo") \
        .agg(sum("value.TotalValue").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    kafka_target_df = rewards_df.select(expr("CustomerCardNo as key"),
                                        to_json(struct("TotalPurchase", "AggregatedRewards")).alias(
                                            "value")
                                        )

    rewards_writer_query = kafka_target_df.writeStream \
        .queryName("Rewards Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "customer-rewards") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    rewards_writer_query.awaitTermination()
