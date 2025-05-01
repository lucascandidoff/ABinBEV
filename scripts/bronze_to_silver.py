from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, upper, trim
from delta.tables import DeltaTable

def transform_to_silver():
    BRONZE_PATH = "data/bronze/breweries_bronze"
    SILVER_PATH = "data/silver/breweries_silver"

    spark = SparkSession.builder \
        .appName("Transform Bronze to Silver") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)

    df_transformed = df_bronze.select(
        "id", "name", "brewery_type", "city", "state", "country",
        "latitude", "longitude", "website_url", "ingestion_date"
    ).withColumn("city", upper(trim(col("city")))) \
     .withColumn("state", upper(trim(col("state")))) \
     .withColumn("country", upper(trim(col("country")))) \
     .withColumn("updated_at", current_date())

    # Check if silver table already exists
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        delta_silver = DeltaTable.forPath(spark, SILVER_PATH)

        # Perform upsert
        delta_silver.alias("target").merge(
            df_transformed.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdate(set={
            "name": "source.name",
            "brewery_type": "source.brewery_type",
            "city": "source.city",
            "state": "source.state",
            "country": "source.country",
            "latitude": "source.latitude",
            "longitude": "source.longitude",
            "website_url": "source.website_url",
            "ingestion_date": "source.ingestion_date",
            "updated_at": "source.updated_at"
        }).whenNotMatchedInsert(values={
            "id": "source.id",
            "name": "source.name",
            "brewery_type": "source.brewery_type",
            "city": "source.city",
            "state": "source.state",
            "country": "source.country",
            "latitude": "source.latitude",
            "longitude": "source.longitude",
            "website_url": "source.website_url",
            "ingestion_date": "source.ingestion_date",
            "updated_at": "source.updated_at",
            "inserted_at": current_date()
        }).execute()
    else:
        # Add inserted_at and write the table for the first time
        df_transformed = df_transformed.withColumn("inserted_at", current_date())
        df_transformed.write.format("delta") \
            .partitionBy("state", "city") \
            .save(SILVER_PATH)

    print(f"Silver table upsert completed at {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    transform_to_silver()