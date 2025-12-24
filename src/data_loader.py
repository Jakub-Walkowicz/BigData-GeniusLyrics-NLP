from pyspark.sql import SparkSession
from pathlib import Path


def get_dir_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "data" / "raw"


class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.raw_data_path = get_dir_path() / "song_lyrics.csv"

    def load_raw_csv(self):
        return (self.spark.read
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("inferSchema", "true")
                .csv(str(self.raw_data_path)))

