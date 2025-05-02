from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, upper, trim, when
from delta.tables import DeltaTable
from pyspark.sql.types import StringType, LongType

def run():

    BRONZE_PATH = "data/bronze/breweries"
    SILVER_PATH = "data/silver/breweries"

    spark = SparkSession.builder \
        .appName("Transform Bronze to Silver") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)

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
                        .withColumn("updated_at", current_date())\
                        .select(
                            "id", "name", "brewery_type", "phone", "address", "address_2", "address_3",
                            "city", "state", "state_province", "postal_code", "country", "latitude", 
                            "longitude", "website_url", "ingestion_date", "updated_at"
                        )

    # Check if silver table already exists
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        delta_silver = DeltaTable.forPath(spark, SILVER_PATH)

        join_condition = "target.id = source.id"
        
        source_columns = [col for col in df_transformed.columns if col != "id"]
        set_dict = {col: f"source.{col}" for col in source_columns}

        insert_dict = {col: f"source.{col}" for col in df_transformed.columns}
        insert_dict["inserted_at"] = current_date()
        
        delta_silver.merge(df_transformed, join_condition)\
                    .whenMatchedUpdate(set=set_dict)\
                    .whenNotMatchedInsert(values=insert_dict)\
                    .execute()
    else:
        # Add inserted_at and write the table for the first time
        df_transformed = df_transformed.withColumn("inserted_at", current_date())
        
        df_transformed.write.format("delta") \
                            .partitionBy("state", "city") \
                            .save(SILVER_PATH)   

    print(f"Silver table upsert completed at {SILVER_PATH}")
    spark.stop()

if __name__ == "__main__":
    run()