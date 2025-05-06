from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, upper, trim, when
from delta.tables import DeltaTable
from pyspark.sql.types import StringType, LongType


BRONZE_PATH = "data/bronze/breweries"
SILVER_PATH = "data/silver/breweries"

# spark = SparkSession.builder \
#     .appName("Transform Bronze to Silver") \
#     .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Transform Bronze to Silver") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


df = spark.read.format("delta").load(SILVER_PATH)

df.show(truncate=False)
df.printSchema()

#df_bronze = spark.read.format("delta").load(BRONZE_PATH)