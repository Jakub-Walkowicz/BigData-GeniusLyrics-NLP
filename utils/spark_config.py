from pyspark.sql import SparkSession

def create_spark_session():
    spark = (SparkSession.builder
        .master("local[*]")
        .appName("GeniusLyricsBigDataProject")
        .config("spark.driver.memory", "6g")
        # .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=4g")
        # .config("spark.driver.maxResultSize", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

