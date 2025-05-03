import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

API_URL = "https://api.openbrewerydb.org/v1/breweries"
BRONZE_PATH = "data/bronze/breweries"

def run():
    spark = SparkSession.builder \
        .appName("ExtractBreweryData") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Convert JSON to DataFrame
        # df_raw = spark.read.json(spark.sparkContext.parallelize(data))

        schema = StructType([
            StructField("id", StringType(), True), # True indica que o campo é nullable (pode ser None)
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True), # Mantido como StringType devido à variação e Nones
            StructField("country", StringType(), True),
            StructField("longitude", DoubleType(), True), # Usando DoubleType para coordenadas
            StructField("latitude", DoubleType(), True),   # Usando DoubleType para coordenadas
            StructField("phone", StringType(), True),     # Mantido como StringType devido à variação e Nones
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), True),
            StructField("street", StringType(), True)
        ])

        # Criar o DataFrame a partir da lista de dicionários, passando o esquema
        df_raw = spark.createDataFrame(data, schema=schema)

        df_raw = df_raw.withColumn("ingestion_date", current_timestamp())

        # Write to Delta
        df_raw.write.format("delta").mode("overwrite").save(BRONZE_PATH)
        print(f"Raw data written to Delta at {BRONZE_PATH}")

    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    run()
