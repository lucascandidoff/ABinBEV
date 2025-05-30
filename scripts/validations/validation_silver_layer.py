from pyspark.sql import SparkSession

# Path to the Delta table
SILVER_PATH = "data/silver/breweries"

# Start a Spark session with Delta support
spark = SparkSession.builder \
    .appName("Transform Bronze to Silver") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load(SILVER_PATH)
df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("data/csv/breweries_silver.csv")

df.printSchema()

# Optional: print schema
# df.printSchema()

spark.stop()
