from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, upper, trim, when, expr
from delta.tables import DeltaTable
from pyspark.sql.types import StringType, LongType

def run():
    """
    Transforms the data from the Bronze layer (Delta format) to the Silver layer (Delta format).

    This function performs the following steps:
    1. Reads the raw brewery data from the Bronze layer, stored in Delta format.
    2. Transforms the data by:
        - Casting and cleaning up columns (e.g., trimming, converting to uppercase).
        - Handling null values by setting default values for latitude and longitude.
        - Adding new columns (`updated_at` and `ingestion_date`).
    3. Checks if a Delta table exists at the Silver layer path:
        - If the table exists, it performs an upsert operation, updating existing records and inserting new ones.
        - If the table does not exist, it writes the transformed data to the Silver layer for the first time.
    4. Partitions the Silver table by "state" to optimize query performance.

    The function also includes error handling to capture and print any issues during the transformation and upsert process.

    Raises:
        Exception: If an error occurs during the reading, transformation, or writing process.
    """

    # Define paths for the Bronze and Silver layers
    # BRONZE_PATH = "data/bronze/breweries"  for run localy
    BRONZE_PATH = "/opt/airflow/data/bronze/breweries"  
    SILVER_PATH = "data/silver/breweries"

    spark = SparkSession.builder \
        .appName("Transform Bronze to Silver") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Read data from the Bronze Delta table
        df_bronze = spark.read.format("delta").load(BRONZE_PATH)

        # Data transformation: cleaning and casting columns
        df_transformed = df_bronze.withColumn("id", col("id").cast(StringType())) \
                            .withColumn("postal_code", col("postal_code").cast(StringType())) \
                            .withColumn("address", upper(trim(col('street')).cast(StringType()))) \
                            .withColumn("address_2", upper(trim(col('address_2')).cast(StringType()))) \
                            .withColumn("address_3", upper(trim(col('address_3')).cast(StringType()))) \
                            .withColumn("phone", upper(trim(col('phone')).cast(StringType()))) \
                            .withColumn("website_url", upper(trim(col('website_url')).cast(StringType()))) \
                            .withColumn("name", upper(trim(col('name')).cast(StringType()))) \
                            .withColumn("brewery_type", upper(trim(col('brewery_type')).cast(StringType()))) \
                            .withColumn("latitude", when(col("latitude").isNull(), 0).otherwise(col("latitude")).cast(LongType())) \
                            .withColumn("longitude", when(col("longitude").isNull(), 0).otherwise(col("longitude")).cast(LongType())) \
                            .withColumn("city", upper(trim(col("city")).cast(StringType()))) \
                            .withColumn("state", upper(trim(col("state")).cast(StringType()))) \
                            .withColumn("state_province", upper(trim(col("state_province")).cast(StringType()))) \
                            .withColumn("country", upper(trim(col("country")).cast(StringType()))) \
                            .withColumn("updated_at", current_date()) \
                            .select(
                                "id", "name", "brewery_type", "phone", "address", "address_2", "address_3",
                                "city", "state", "state_province", "postal_code", "country", "latitude", 
                                "longitude", "website_url", "ingestion_date", "updated_at"
                            )

        df_transformed.printSchema()

    except Exception as e:
        print(f"Error to process bronze: {e}")

    # Check if Silver table already exists
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):

        try:
            source_df = df_transformed.alias("source")
            target_df = DeltaTable.forPath(spark, SILVER_PATH)

            join_condition = expr("target.id = source.id")

            # Create dictionaries for the merge operation
            source_columns = [c for c in df_transformed.columns if c != "id"]
            set_dict = {c: col(f"source.{c}") for c in source_columns}

            insert_dict = {c: col(f"source.{c}") for c in df_transformed.columns}
            insert_dict["inserted_at"] = current_date()

            # Perform the merge (upsert) operation
            target_df.alias("target") \
                .merge(source_df, join_condition) \
                .whenMatchedUpdate(set=set_dict) \
                .whenNotMatchedInsert(values=insert_dict) \
                .execute()

        except Exception as e:
            print(f"Error in the process: {e}")
            raise

    else:
        # Write the transformed data to the Silver table for the first time
        df_transformed = df_transformed.withColumn("inserted_at", current_date())
        
        df_transformed.write.format("delta") \
                            .partitionBy("state") \
                            .save(SILVER_PATH)

    print(f"Silver table upsert completed at {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    run()
