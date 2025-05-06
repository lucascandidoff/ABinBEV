from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, count
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, TimestampType
import os

def run():
    """
    This function performs the transformation of data from the Silver layer to the Gold layer
    in a data lake. It processes brewery data to create dimensional tables and a fact table for
    a dimensional model. The transformation involves creating:
    - `dim_brewery`: A dimension table for brewery-related information.
    - `dim_location`: A dimension table for location-related information.
    - `fact_brewery`: A fact table linking brewery and location data.

    The function follows these steps:
    1. Reads the data from the Silver layer.
    2. Creates the `dim_brewery` dimension with unique brewery IDs.
    3. Creates the `dim_location` dimension with unique location IDs.
    4. Joins the brewery and location data to create the `fact_brewery` fact table.
    5. Saves the resulting dimension and fact tables to the Gold layer.

    Returns:
        None
    """
    
    spark = SparkSession.builder \
        .appName("Transform Silver to Gold") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # SILVER_PATH = "data/silver/breweries" # for run localy
    SILVER_PATH = "/opt/airflow/data/silver/breweries"  
    GOLD_PATH = "data/gold"

    # Read silver table
    df_silver = spark.read.format("delta").load(SILVER_PATH)

    # ----------------------------
    # Dimension: dim_brewery
    # ----------------------------
    dim_brewery = df_silver.select(
        "id", "name", "brewery_type", "phone", "website_url"
    ).dropDuplicates()

    window_brewery = Window.orderBy("id")
    dim_brewery = dim_brewery.withColumn("brewery_id", row_number().over(window_brewery))

    dim_brewery = dim_brewery.select(
        "brewery_id", "id", "name", "brewery_type", "phone", "website_url"
    )

    schema_dim_brewery = StructType([
        StructField("brewery_id", LongType(), False),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
    ])

    dim_brewery_typed = spark.createDataFrame(dim_brewery.rdd, schema=schema_dim_brewery)

    dim_brewery_typed.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_brewery")
    print(f"Dimension table saved to {GOLD_PATH}/dim_brewery")

    # ----------------------------
    # Dimension: dim_location
    # ----------------------------
    dim_location = df_silver.select(
        "address", "address_2", "address_3", "city",
        "state", "state_province", "postal_code", "country",
        col("latitude").cast("long"),
        col("longitude").cast("long")
    ).dropDuplicates()

    window_location = Window.orderBy("city", "state", "postal_code")
    dim_location = dim_location.withColumn("location_id", row_number().over(window_location))

    dim_location = dim_location.select(
        "location_id", "address", "address_2", "address_3", "city",
        "state", "state_province", "postal_code", "country",
        "latitude", "longitude"
    )

    schema_dim_location = StructType([
        StructField("location_id", LongType(), False),
        StructField("address", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", LongType(), True),
        StructField("longitude", LongType(), True),
    ])

    dim_location_typed = spark.createDataFrame(dim_location.rdd, schema=schema_dim_location)

    dim_location_typed.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_location")
    print(f"Dimension table saved to {GOLD_PATH}/dim_location")


    # ----------------------------
    # Fact table: fact_brewery
    # ----------------------------
    fact = df_silver.alias("s") \
        .join(dim_brewery_typed.alias("b"),
            (col("s.id") == col("b.id")) &
            (col("s.name") == col("b.name")),
            "left") \
        .join(dim_location_typed.alias("l"),
            (col("s.address") == col("l.address")) &
            (col("s.city") == col("l.city")) &
            (col("s.state") == col("l.state")),
            "left")

    fact_brewery = fact.select(
        "s.id", "s.name", "s.brewery_type", "s.phone", "l.address", "l.address_2", "l.address_3",
        "l.city", "l.state", "l.state_province", "l.postal_code", "l.country", "l.latitude", "l.longitude",
        "s.website_url", "s.ingestion_date", "s.updated_at", "s.inserted_at", "b.brewery_id", "l.location_id"
    )

    window_fact = Window.orderBy("s.id")
    fact_brewery = fact_brewery.withColumn("fact_brewery_id", row_number().over(window_fact))

    fact_brewery = fact_brewery.select(
        "fact_brewery_id", "id", "name", "brewery_type", "phone",
        "address", "address_2", "address_3", "city", "state", "state_province",
        "postal_code", "country", "latitude", "longitude",
        "website_url", "ingestion_date", "updated_at", "inserted_at",
        "brewery_id", "location_id"
    )

    schema_fact_brewery = StructType([
        StructField("fact_brewery_id", LongType(), False),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", LongType(), True),
        StructField("longitude", LongType(), True),
        StructField("website_url", StringType(), True),
        StructField("ingestion_date", TimestampType(), True),
        StructField("updated_at", DateType(), True),
        StructField("inserted_at", DateType(), True),
        StructField("brewery_id", LongType(), True),
        StructField("location_id", LongType(), True),
    ])

    fact_brewery_typed = spark.createDataFrame(fact_brewery.rdd, schema=schema_fact_brewery)

    fact_brewery_typed.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/fact_brewery")
    print(f"Aggregated table saved to {GOLD_PATH}/fact_brewery")


    # ----------------------------
    # Aggregated Table: brewery_counts_by_location_type
    # ----------------------------
    # Aggregate directly from the fact table or silver depending on data needed
    # Using fact_brewery_typed which includes the cleaned location/type data
    df_aggregated = fact_brewery_typed.groupBy("brewery_type", "city", "state_province") \
                            .agg(count("*").alias("quantity_breweries")) \
                            .orderBy("brewery_type", "state_province", "city") 

    schema_aggregated = StructType([
        StructField("brewery_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("quantity_breweries", LongType(), False)
    ])

    df_aggregated_typed = spark.createDataFrame(df_aggregated.rdd, schema=schema_aggregated)

    df_aggregated_typed.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/brewery_counts_by_location_type")
    print(f"Aggregated table saved to {GOLD_PATH}/brewery_counts_by_location_type")

    spark.stop()


if __name__ == "__main__":
    run()
