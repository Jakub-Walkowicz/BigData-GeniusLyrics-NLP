from pyspark.sql import SparkSession
import os

def create_spark_session():
    spark = (SparkSession.builderaaaa
        .master("local[*]")
        .appName("GeniusLyricsBigDataProject")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    path_to_nlp = os.path.abspath("../src/nlp_processor.py")
    spark.sparkContext.addPyFile(path_to_nlp)
    spark.sparkContext.setLogLevel("ERROR")
    return spark

