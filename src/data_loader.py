from pyspark.sql import SparkSession
from pathlib import Path

def get_dir_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "data"

class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.raw_csv_path = get_dir_path() / "raw" / "song_lyrics.csv"
        self.parquet_path = get_dir_path() / "processed" / "song_lyrics.parquet"
        self.parquet_sample_path = get_dir_path() / "processed" / "test_sample.parquet"

    def load_raw_csv(self):
        return (self.spark.read
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("inferSchema", "true")
                .csv(str(self.raw_csv_path)))

    def load_parquet(self):
        return self.spark.read.parquet(str(self.parquet_path))

    def load_sample(self):
        return self.spark.read.parquet(str(self.parquet_sample_path))
