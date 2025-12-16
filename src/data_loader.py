from constants.constants import SCHEMA
from pyspark.sql import SparkSession
from pathlib import Path

class DataLoader:
    def __init__(self, spark: SparkSession):
        self.path = self._get_path()
        self.spark = spark

    def load_file(self):
        return self.spark.read.csv(
            self.path,
            schema=SCHEMA,
            header=True,
        )

    def _get_path(self):
        base_dir = Path(__file__).resolve().parent.parent
        return str(base_dir / "data" / "raw" / "song_lyrics.csv")