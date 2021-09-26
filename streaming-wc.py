from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("streaming word count") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    # read
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # lines_df.printSchema()

    # transform
    words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
    counts_df = words_df.groupBy("word").count()

    # write
    word_count_query = counts_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("complete") \
        .start()
    
    word_count_query.awaitTermination()