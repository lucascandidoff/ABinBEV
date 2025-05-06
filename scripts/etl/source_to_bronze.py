import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

API_URL = "https://api.openbrewerydb.org/v1/breweries"
BRONZE_PATH = "data/bronze/breweries"

def run():
    """
    Extracts brewery data from the Open Brewery DB API, processes it into a Spark DataFrame, 
    and writes it to a Delta table in the Bronze layer.

    The function performs the following steps:
    1. Initializes a SparkSession with Delta configurations.
    2. Makes a GET request to the Open Brewery DB API to fetch brewery data.
    3. Defines a schema for the data structure and creates a DataFrame.
    4. Adds an "ingestion_date" column to capture the timestamp of data ingestion.
    5. Writes the DataFrame to Delta format at the specified BRONZE_PATH in overwrite mode.
    6. Handles potential errors during the API request and data processing.
    7. Stops the SparkSession after the process is completed.

    Raises:
        requests.RequestException: If there is an error during the API request.
    """

    spark = SparkSession.builder \
        .appName("ExtractBreweryData") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Fetch data from the API
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Define the schema for the expected data structure
        schema = StructType([
            StructField("id", StringType(), True), 
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", DoubleType(), True), 
            StructField("latitude", DoubleType(), True),  
            StructField("phone", StringType(), True),   
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), True),
            StructField("street", StringType(), True)
        ])

        # Create a DataFrame from the API data
        df_raw = spark.createDataFrame(data, schema=schema)

        # Add ingestion timestamp column
        df_raw = df_raw.withColumn("ingestion_date", current_timestamp())

        # Write the DataFrame to Delta
        df_raw.write.format("delta").mode("overwrite").save(BRONZE_PATH)
        print(f"Raw data written to Delta at {BRONZE_PATH}")

    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    run()
