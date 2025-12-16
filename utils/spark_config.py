from pyspark.sql import SparkSession

def create_spark_session():
    spark = (SparkSession.builder
        .master("local[*]")
        .appName("GeniusLyricsBigDataProject")
        .getOrCreate()
    )
    return spark

