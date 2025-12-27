from cassandra.cluster import Cluster

def setup_cassandra_db():
    cluster = Cluster(['cassandra'])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS genius_space
        WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
    """)

    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS genius_space.processed_lyrics (

def save_processed_lyrics():
    pass

def load_processed_lyrics():
    pass