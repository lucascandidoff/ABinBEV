from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import os

def run():
    
    # Create Spark session with Delta support    
    spark = SparkSession.builder \
        .appName("ExtractBreweryData") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


    # Paths for silver and gold layers
    # silver_path = os.getenv("SILVER_PATH", "data/silver/breweries")
    # gold_path = os.getenv("GOLD_PATH", "data/gold")

    silver_path = "data/silver/breweries"
    gold_path = "data/gold"

    # Read silver table
    df_silver = spark.read.format("delta").load(silver_path)

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

    dim_brewery.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_brewery")        

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

    dim_location.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_location")

    # Select dim_location with location_id as the first column
    dim_location_selected = dim_location.select(
        "location_id", "address", "address_2", "address_3", "city",
        "state", "state_province", "postal_code", "country",
        "latitude", "longitude"
    )

    # ----------------------------
    # Fact table: fact_brewery
    # ----------------------------
    fact = df_silver.alias("s") \
        .join(dim_brewery.alias("b"),
              (col("s.id") == col("b.id")) &
              (col("s.name") == col("b.name")),
              "left") \
        .join(dim_location.alias("l"),
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

    fact_brewery.write.mode("overwrite").format("delta").save(f"{gold_path}/fact_brewery")

    spark.stop()

# Optional: allow local execution
if __name__ == "__main__":
    run()
