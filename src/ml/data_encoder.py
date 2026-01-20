from pyspark.ml.feature import OneHotEncoder, StringIndexer 
from pyspark.ml import Pipeline

def encode_label(df, col: str):
    indexer_output_col = col+"_index"
    indexer = StringIndexer(inputCol=col, outputCol=indexer_output_col)
    return indexer.fit(df).transform(df)
