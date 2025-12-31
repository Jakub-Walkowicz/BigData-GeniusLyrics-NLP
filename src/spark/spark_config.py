from pyspark.sql import SparkSession

def create_spark_session():
    return (SparkSession.builder
        .appName("GeniusLyricsBigDataProject")
        .master("spark://spark-master:7077")
        .config("spark.submit.pyFiles", "/opt/spark/work-dir/src")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")
        .config("spark.executor.memory", "3g")
        .config("spark.driver.memory", "1500m")
        .getOrCreate()
    )
