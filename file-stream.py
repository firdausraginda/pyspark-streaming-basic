from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    raw_df = spark.readStream \
        .format("json") \
        .option("path", "src") \
        .option("maxFilesPerTrigger", 1) \
        .option("cleanSource", "delete") \
        .load()

    # raw_df.printSchema()

    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                   "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                   "DeliveryAddress.State", "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem"
                                   )
    
    # explode_df.printSchema()

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .queryName("Flattened Invoice Writer") \
        .trigger(processingTime="1 minute") \
        .start()

    invoice_writer_query.awaitTermination()