import requests
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession, current_timestamp
from pyspark.sql.functions import lit

# Define API endpoint and Delta output path
API_URL = "https://api.openbrewerydb.org/v1/breweries"
BRONZE_PATH = "data/bronze/breweries_bronze"

def extract_data():
    # Create Spark session with Delta support
    spark = SparkSession.builder \
        .appName("ExtractBreweryData") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Fetch data from the API
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Convert JSON to temporary DataFrame
        rdd = spark.sparkContext.parallelize([json.dumps(data)])
        df_raw = spark.read.json(rdd)

        # Optionally add metadata
        df_raw = df_raw.withColumn("ingestion_date", current_timestamp())

        # Write to Delta format
        df_raw.write.format("delta").mode("append").save(BRONZE_PATH)

        print(f"Raw data written to Delta at {BRONZE_PATH}")

    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        raise

    finally:
        spark.stop()


# Entry point
if __name__ == "__main__":
    extract_data()
