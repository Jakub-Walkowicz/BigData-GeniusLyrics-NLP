from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class EdaAnalyser:
    def __init__(self, df: DataFrame):
        self.df = df

    def _print_null_report(self, columns):
        print("Number of null values: ")
        (self.df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).show())

    def _print_basic_report(self):
        print("Columns and their datatypes present in the dataset:")
        self.df.printSchema()
        print("Sample 5 rows:")
        (self.df.show(5))
        print(f'Dimension of the Dataframe is: {(self.df.count(), len(self.df.columns))}')

    def _print_top_songs(self, lang):
        print(f"Top 5 most viewed {lang} songs:")
        (self.df.filter(F.col('language') == lang)
         .select('title', 'artist', 'views')
         .sort(F.col('views').desc())
         .limit(5)
         .show())

    def run_full_eda_report(self, columns):
        self.df.cache()
        self._print_basic_report()
        self._print_null_report(columns)
        self._print_top_songs('pl')
        self._print_top_songs('en')
        self.df.unpersist()
