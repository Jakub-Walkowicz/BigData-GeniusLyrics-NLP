import pyspark.sql.functions as F

class Preprocessor:
    @staticmethod
    def run(df):
        df = df.na.drop(subset=['lyrics', 'artist', 'views'])
        df = df.filter(F.col('language') == 'pl')
        return df.withColumn('lyrics_cleaned', Preprocessor._clean_text_logic(F.col('lyrics')))

    @staticmethod
    def _clean_text_logic(col):
        col = F.regexp_replace(col, r'\[[^\]]*\]', '')
        col = F.regexp_replace(col, r'\([^)]*\)', '')
        col = F.regexp_replace(col, r'[‘’`´]', "'")
        col = F.regexp_replace(col, r'[ \t]+', ' ')
        col = F.regexp_replace(col, r' *\n *', '\n')
        col = F.regexp_replace(col, r'^\s+', '')
        col = F.regexp_replace(col, r'\s+$', '')
        return F.trim(col)
