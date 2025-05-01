from pyspark.sql import SparkSession

# Path to the Delta table
BRONZE_PATH = "data/bronze/breweries_bronze"

# Start a Spark session with Delta support
spark = SparkSession.builder \
    .appName("Check Bronze Data") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Load Delta table
df = spark.read.format("delta").load(BRONZE_PATH)

df.printSchema()

# Show the first rows
# df.show(truncate=False)

# Optional: print schema
# df.printSchema()

spark.stop()
