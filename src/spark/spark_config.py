from pyspark.sql import SparkSession

def create_spark_session():
    return (SparkSession.builder
        .appName("GeniusLyricsBigDataProject")
        .master("spark://spark-master:7077")
        .config("spark.submit.pyFiles", "/opt/spark/work-dir/src/nlp_processor.py")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
