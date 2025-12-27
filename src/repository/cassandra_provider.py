from cassandra.cluster import Cluster

class CassandraProvider():
    def __init__(self, spark, keyspace='genius_space', table='processed_songs'):
        self.spark = spark,
        self.keyspace = keyspace
        self.table = table

    @staticmethod
    def setup_cassandra():
        cluster = Cluster(['cassandra'])
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS genius_space
            WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS genius_space.processed_songs (
                id BIGINT PRIMARY KEY,
                artist TEXT,
                title TEXT,
                views BIGINT,
                tag TEXT,
                tfidf_array list<DOUBLE>
            )
        """)


    def save(self, df):
        pass

    def load(self):
        pass