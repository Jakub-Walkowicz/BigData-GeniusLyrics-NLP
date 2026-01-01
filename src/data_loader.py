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
        if not self.raw_csv_path.exists():
            raise FileNotFoundError(f"Source file not found: {self.raw_csv_path}")
        return (self.spark.read
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("inferSchema", "true")
                .csv(str(self.raw_csv_path)))

    def create_parquet(self):
        df = self.load_raw_csv()
        df.write.parquet(str(self.parquet_path))

    def load(self):
        if not any(self.parquet_path.glob("*.parquet")):
            print("Parquet not found. Generating...")
            self.create_parquet()
        return self.spark.read.parquet(str(self.parquet_path))

    def load_sample(self):
        return self.spark.read.parquet(str(self.parquet_sample_path))
