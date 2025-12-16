from pyspark.sql.types import StructType, StructField, StringType, IntegerType

SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("views", IntegerType(), True),
    StructField("features", StringType(), True),
    StructField("lyrics", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("language_cld3", StringType(), True),
    StructField("language_ft", StringType(), True),
    StructField("language", StringType(), True),
])