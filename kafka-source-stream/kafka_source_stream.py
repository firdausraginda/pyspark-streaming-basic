from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming from Kafka") \
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
        StructField("TotalAmount", StringType()),
        StructField("NumberOfItems", DoubleType()),
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
        ])))
    ])
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    # debugging
    # kafka_df.printSchema()
    # kafka_df.select(col('value').cast('string')).show(5, False) # by default, 'value' has binary data type, need to convert to string before show it
    
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    # value_df.printSchema()
    
    
    # pyspark sql expression version
    explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.PosID", "value.CustomerType",
                                    "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
                                    "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", 
                                    "explode(value.InvoiceLineItems) as LineItem")

    # python version
    # explode_df = value_df.select(col("value.InvoiceNumber").alias("InvoiceNumber"), col("value.CreatedTime").alias("CreatedTime"), 
    #                                 col("value.PosID").alias("PosID"), col("value.CustomerType").alias("CustomerType"),
    #                                 col("value.PaymentMethod").alias("PaymentMethod"), col("value.DeliveryType").alias("DeliveryType"), 
    #                                 col("value.DeliveryAddress.City").alias("City"), col("value.DeliveryAddress.State").alias("State"), 
    #                                 col("value.DeliveryAddress.PinCode").alias("PinCode"), 
    #                                 explode("value.InvoiceLineItems").alias("LineItem"))
    # explode_df.show(5, False) # debugging


    # pyspark sql expression version
    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    # python version
    # flattened_df = explode_df \
    #     .withColumn("ItemCode", col("LineItem.ItemCode")) \
    #     .withColumn("ItemDescription", col("LineItem.ItemDescription")) \
    #     .withColumn("ItemPrice", col("LineItem.ItemPrice")) \
    #     .withColumn("ItemQty", col("LineItem.ItemQty")) \
    #     .withColumn("TotalValue", col("LineItem.TotalValue")) \
    #     .drop("LineItem")
    # flattened_df.show(5, False) # debugging
    
    
    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    invoice_writer_query.awaitTermination()

