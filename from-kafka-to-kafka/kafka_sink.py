from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Kafka Sink") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))


    # pyspark sql expression version
    notification_df = value_df.selectExpr("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount", 
                "(value.TotalAmount * 0.2) as EarnedLoyaltyPoints")

    # python version
    # notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
    #     .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))
    # notification_df.show(5, False)


    # pyspark sql expression version
    kafka_target_df = notification_df.selectExpr("InvoiceNumber as key",
                                                """to_json(named_struct(
                                                'CustomerCardNo', CustomerCardNo,
                                                'TotalAmount', TotalAmount,
                                                'EarnedLoyaltyPoints', TotalAmount * 0.2)) as value""")
    
    # python version
    # kafka_target_df = notification_df.select(col("InvoiceNumber").alias("key"),
    #                                             to_json(expr("""
    #                                                 named_struct(
    #                                                     'CustomerCardNo', CustomerCardNo,
    #                                                     'TotalAmount', TotalAmount,
    #                                                     'EarnedLoyaltyPoints', EarnedLoyaltyPoints
    #                                                 )
    #                                                 """)).alias("value"))
    # kafka_target_df.show(5, False) # debugging


    notification_writer_query = kafka_target_df \
        .writeStream \
        .queryName("Notification Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "notifications") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    notification_writer_query.awaitTermination()