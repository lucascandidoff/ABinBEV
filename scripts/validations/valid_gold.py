from pyspark.sql import SparkSession

# Path to the Delta table
dim_brewery = "data/gold/dim_brewery"
dim_location = "data/gold/dim_location"
fact_brewery = "data/gold/fact_brewery"

# Start a Spark session with Delta support
spark = SparkSession.builder \
    .appName("Check Silver Data") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Creating CSV dim_brewery
df = spark.read.format("delta").load(dim_brewery)
df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("data/csv/dim_brewery.csv")
print("\n-----------------------------------------\n")
print("dim_brewery")
print(df.printSchema())
print("\n-----------------------------------------\n")

# Creating CSV dim_location
df = spark.read.format("delta").load(dim_location)
df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("data/csv/dim_location.csv")
print("\n-----------------------------------------\n")
print("dim_location")
print(df.printSchema())
print("\n-----------------------------------------\n")


# Creating CSV fact_brewery
df = spark.read.format("delta").load(fact_brewery)
df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("data/csv/fact_brewery.csv")
print("\n-----------------------------------------\n")
print("fact_brewery")
print(df.printSchema())
print("\n-----------------------------------------\n")

# Optional: print schema
# df.printSchema()

spark.stop()
