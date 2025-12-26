from pyspark.sql import SparkSession
import os

def create_spark_session():
    spark = (SparkSession.builder
        .appName("GeniusLyricsBigDataProject")
        .master("spark://spark-master:7077")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    path_to_nlp = os.path.abspath("../nlp_processor.py")
    spark.sparkContext.addPyFile(path_to_nlp)
    spark.sparkContext.setLogLevel("ERROR")
    return spark

