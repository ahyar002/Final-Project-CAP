from pyspark.sql import SparkSession
import subprocess
import pysolr
import json

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Connecting to Solr") \
    .getOrCreate()

def connecting_to_solr(core):
    # Command to create Solr core
    create_core = f"/home/ahyar/hadoop/Solr/bin/solr create -c {core}"

    # Check if Solr core already exists
    solr_core_exists = subprocess.run(create_core, shell=True, capture_output=True).returncode != 0

    if solr_core_exists:
        print("Solr core already exists.")
    else:
        print("Creating Solr core...")
        subprocess.run(create_core, shell=True)

    # Load data from HDFS as DataFrame
    df = spark.read.format("parquet").option("header", "true").load(f"hdfs://localhost:9000/user/hive/warehouse/dm_3.db/{core}/*.parquet")

    #df = df.drop('_c0')

    # Convert DataFrame to list of dictionaries
    documents = df.toJSON().collect()

    # Convert JSON strings to dictionaries
    documents = [json.loads(doc) for doc in documents]

    # Create a Solr client
    solr = pysolr.Solr(f'http://localhost:8983/solr/{core}')

    # Delete existing documents(MUST)
    solr.delete(q="*:*")

    # Index documents into Solr
    solr.add(documents)

    # Index the valid documents
    #solr.add(valid_documents)

    # Commit the changes
    solr.commit()

connecting_to_solr("data_master")
connecting_to_solr("data_mart_1")
connecting_to_solr("data_mart_2")


# valid_documents = []
    # for doc in documents:
    #     if 'word_count' in doc and doc['word_count'].strip().isdigit():
    #         valid_documents.append(doc)
    #     else:
    #         # Handle missing or invalid 'word_count' value
    #         doc['word_count'] = 0
    #         valid_documents.append(doc)
