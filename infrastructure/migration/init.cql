CREATE KEYSPACE IF NOT EXISTS genius_space
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS genius_space.processed_songs (
    id BIGINT PRIMARY KEY,
    artist TEXT,
    title TEXT,
    views BIGINT,
    tag TEXT,
    feature_array list<DOUBLE>
);