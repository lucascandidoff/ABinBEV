from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def transform_to_gold():
    
    # Path to the Delta table
    SILVER_PATH = "data/silver/breweries_silver"
    GOLD_PATH = "data/gold"

    spark = SparkSession.builder \
        .appName("Transform Bronze to Silver") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Read silver layer
    df_silver = spark.read.format("delta").load(SILVER_PATH)

    # === Brewery Dimension ===
    dim_brewery = df_silver.select("id", "name", "brewery_type", "website_url").distinct() \
        .withColumnRenamed("id", "brewery_id")

    # Add sequential ID starting from 1
    dim_brewery = dim_brewery.withColumn(
        "id_brewery",
        row_number().over(Window.orderBy("brewery_id"))
    )

    dim_brewery.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_brewery")

    # === Location Dimension ===
    dim_location = df_silver.select("city", "state", "country", "latitude", "longitude").distinct()

    dim_location = dim_location.withColumn(
        "id_location",
        row_number().over(Window.orderBy("city", "state", "country"))
    )

    dim_location.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_location")

    # === Brewery Fact Table ===
    fact_brewery = df_silver.alias("f") \
        .join(dim_brewery.alias("b"), col("f.id") == col("b.brewery_id"), "left") \
        .join(dim_location.alias("l"),
              (col("f.city") == col("l.city")) &
              (col("f.state") == col("l.state")) &
              (col("f.country") == col("l.country")) &
              (col("f.latitude") == col("l.latitude")) &
              (col("f.longitude") == col("l.longitude")), "left") \
        .select(
            col("f.id").alias("external_id"),
            col("b.id_brewery"),
            col("l.id_location"),
            col("f.ingestion_date"),
            col("f.updated_at"),
            col("f.inserted_at")
        )

    fact_brewery = fact_brewery.withColumn(
        "id_fact",
        row_number().over(Window.orderBy("external_id"))
    )

    fact_brewery.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/fact_brewery")

if __name__ == "__main__":
    transform_to_gold()