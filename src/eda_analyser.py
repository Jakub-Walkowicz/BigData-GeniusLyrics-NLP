from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class EdaAnalyser:
    def __init__(self, df: DataFrame, cols_for_analysis=['title', 'lyrics', 'views']):
        self.df = df.cache()
        self.cols_for_analysis = cols_for_analysis

    def _basic_analysis(self):
        # print("Columns contained in the dataset:")
        # print(self.df.columns)
        # print("Columns datatypes present in the dataset:")
        # self.df.printSchema()
        # print("Viewing top 5 rows:")
        # self.df.filter(
        #     self.df['lyrics'].isNotNull()
        # ).show(5)
        # row_count = self.df.count()
        # col_count = len(self.df.columns)
        # print(f'Dimension of the Dataframe is: {(col_count, row_count)}')
        print("\n")
        print("Number of null values in the dataset:")
        (self.df
            .select([F.count(F.when(F.col(c).isNull(), c)).alias(c)
                for c in self.cols_for_analysis])
            .show()
        )
        print("Top 5 most viewed songs (EN and PL):")
        top_songs = (self.df
                        .filter(F.col('language').isin(['en', 'pl']))
                        .sort(F.col('views').desc())
        )
        print("Top 5 most viewed PL songs:")
        (top_songs
            .filter(F.col('language') == 'pl')
            .limit(5)
            .show()
        )
        print("Top 5 most viewed EN songs:")
        (top_songs
            .filter(F.col('language') == 'en')
            .limit(5)
            .show()
        )



    def perform(self):
        self._basic_analysis()
        self.df.unpersist()