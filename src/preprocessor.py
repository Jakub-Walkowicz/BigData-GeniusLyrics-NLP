import pyspark.sql.functions as F

class Preprocessor:
    @staticmethod
    def run(df):
        return (df
                .transform(Preprocessor._restrict_to_en_songs)
                .transform(Preprocessor._drop_nulls)
                .transform(Preprocessor._remove_annotations))
    @staticmethod
    def _restrict_to_en_songs(df):
        return df.filter(F.col('language') == 'en')

    @staticmethod
    def _drop_nulls(df):
        return df.na.drop()

    @staticmethod
    def _remove_annotations(df):
        return df.withColumn(
            'lyrics_cleaned',
            F.regexp_replace(F.col('lyrics'), r'\[[^\]]*\]', '')
        )
