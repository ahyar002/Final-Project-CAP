from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Staging_in_HIve") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Create database
create_database_staging = """
CREATE DATABASE IF NOT EXISTS nyt_staging
"""

# Create table
create_table_query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS nyt_staging.nyt_table (
        id STRING,
        publication_date STRING,
        web_url STRING,
        headline STRING,
        source STRING,
        author STRING,
        snippet STRING,
        lead_paragraph STRING,
        abstract STRING,
        document_type STRING,
        news_desk STRING,
        section_name STRING,
        subsection_name STRING,
        type_of_material STRING,
        keywords ARRAY<STRING>,
        word_count BIGINT
    )
    STORED AS PARQUET
    LOCATION '/user/ahyar/nyt_staging' 
    TBLPROPERTIES ('skip.header.line.count'='1')
"""

# Insert table
insert_table_property = """
LOAD DATA LOCAL INPATH '/home/ahyar/final_project/artificial-intelligence_Jan-Mei_copy.parquet'
OVERWRITE INTO TABLE nyt_staging.nyt_table
"""

# Execute the query
spark.sql(create_database_staging)
spark.sql(create_table_query)
spark.sql(insert_table_property)
