from pyspark.sql import SparkSession
import time
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when
from pyspark.sql.types import TimestampType
import psutil
import time
import os

#    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/ahyar/datamart") \
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Transformation_and_data_mart") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Start the timer
start_time = time.time()

df = spark.sql("select * from nyt_staging.nyt_table")


# add unique id
# df = df.withColumn("sk", range(1, df.count() + 1))
# df.show()

# Define the format of the publication_date string
date_format = "yyyy-MM-dd HH:mm:ss"

# Convert the publication_date column to timestamp type
df = df.withColumn("publication_date", to_timestamp("publication_date", date_format).cast(TimestampType()))

# Membuat window specification
window = Window.orderBy(F.monotonically_increasing_id())

# Menambahkan kolom surrogate key
df = df.withColumn("sk", F.row_number().over(window))

# drop abstract column because same with snippet column
df = df.drop('abstract')

# Remove "By" from the 'author' column, handling null values as well
df = df.withColumn('author', when(col('author') == 'None', 'None').otherwise(regexp_replace(col('author'), '^By\\s+', '')))

#Rename columns
df = df.withColumnRenamed("type_of_material", "type_article") \
        .withColumnRenamed("id", "article_id")

# data master
data_master = df.orderBy("publication_date")

# data mart sentiment anlysis
data_mart_one = df.selectExpr("sk", "article_id", "publication_date", "headline", "snippet", "lead_paragraph", "keywords").orderBy("publication_date")
#data_mart_one.show()
#data_mart_one.printSchema()

# data mart dashboard
data_mart_two = df.selectExpr("sk", "article_id", "publication_date", "author", "source", "document_type",  "headline", "snippet", "news_desk", "section_name", "keywords", "word_count").orderBy("publication_date")
#data_mart_two.show()

# Create New Database In Hive
spark.sql("CREATE DATABASE IF NOT EXISTS dm_3")
#spark.sql("show databases").show()
spark.sql("use dm_3")

# save in hive
# spark.sql("DROP TABLE IF EXISTS data_master")
# spark.sql("DROP TABLE IF EXISTS data_mart_1")
# spark.sql("DROP TABLE IF EXISTS data_mart_2")

data_master.write.mode("overwrite").saveAsTable("data_master")
data_mart_one.write.mode("overwrite").saveAsTable("data_mart_1")
data_mart_two.write.mode("overwrite").saveAsTable("data_mart_2")

# Stop the timer
end_time = time.time()

# check the memory usage
process = psutil.Process(os.getpid())

# Calculate the execution time
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

# Check the memory usage
memory_usage = (psutil.Process().memory_info().rss)/1024/1024
print(f"Memory usage: {memory_usage:.2f} MB")


