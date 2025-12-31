from cassandra.cluster import Cluster
import pyspark.sql.functions as F

class CassandraProvider():
    def __init__(self, spark, keyspace='genius_space', table='processed_songs'):
        self.spark = spark
        self.keyspace = keyspace
        self.table = table

    def save(self, df):
        required_cols = ["id", "artist", "title", "views", "tag", "tfidf_array"]

        (df.select(*required_cols)
            .write
            .format("org.apache.spark.sql.cassandra")
            .mode('append')
            .options(table=self.table, keyspace=self.keyspace)
            .save())

    def load(self):
        pass