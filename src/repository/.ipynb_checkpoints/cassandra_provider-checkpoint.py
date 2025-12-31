class CassandraProvider():
    def __init__(self, keyspace='genius_space', table='processed_songs'):
        self.keyspace = keyspace
        self.table = table

    def save(self, df):
        required_cols = ["id", "artist", "title", "views", "tag", "feature_array"]

        (df.select(*required_cols)
            .write
            .format("org.apache.spark.sql.cassandra")
            .mode('append')
            .options(table=self.table, keyspace=self.keyspace)
            .save())

    def load(self, spark):
        df = (spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(table=self.table, keyspace=self.keyspace)
          .load())
        return df