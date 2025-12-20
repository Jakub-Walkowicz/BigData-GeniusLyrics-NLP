from pyspark.sql import SparkSession
from pathlib import Path


def get_dir_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "data" / "raw"


class DataLoader:
    def __init__(self, spark: SparkSession):
        self.dir_path = get_dir_path()
        self.spark = spark

    def load_file(self):
        file_path = str(self.dir_path / "song_lyrics_parquet")
        df = self.spark.read.parquet(file_path)
        return df

