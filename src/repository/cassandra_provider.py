from pyspark.ml.functions import vector_to_array
from pyspark.ml.functions import array_to_vector

class CassandraProvider():
    def __init__(self, keyspace='genius_space', table='processed_songs'):
        self.keyspace = keyspace
        self.table = table

    def save(self, df):
        required_cols = ["id", "artist", "title", "views", "tag", "feature_array"]

        df_to_save = df.withColumn("feature_array", vector_to_array("word_vectors")).select(*required_cols)

        (df_to_save.write
            .format("org.apache.spark.sql.cassandra")
            .mode('append')
            .options(table=self.table, keyspace=self.keyspace)
            .save())

    def load(self, spark):
        df = (spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(table=self.table, keyspace=self.keyspace)
          .load())
        df = (df.withColumn("word_vectors", array_to_vector("feature_array"))
              .drop("feature_array"))
        return df