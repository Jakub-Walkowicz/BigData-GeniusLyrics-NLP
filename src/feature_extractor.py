from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.types import DoubleType, ArrayType
from pyspark.sql.functions import pandas_udf
import pandas as pd


@pandas_udf(ArrayType(DoubleType()))
def vector_to_array(series: pd.Series) -> pd.Series:
    return series.apply(lambda v: v.toArray().tolist() if v is not None else None)

class FeatureExtractor:
    def __init__(self, vocabSize, minDF):
        self.model = None
        self.vocabSize = vocabSize
        self.minDF = minDF


    def fit(self, df, inputCol="words_lemmatized"):
        cv = CountVectorizer(
            inputCol=inputCol,
            outputCol="raw_features",
            vocabSize=self.vocabSize,
            minDF=self.minDF
        )
        idf = IDF(inputCol="raw_features", outputCol="tfidf_vectors")
        pipeline = Pipeline(stages=[cv, idf])
        self.model = pipeline.fit(df)
        return self

    def transform(self, df):
        if not self.model:
            raise ValueError("Model must be fitted before transformation.")
        df_transformed = self.model.transform(df)

        return (df_transformed
                .withColumn("tfidf_array", vector_to_array("tfidf_vectors"))
                .drop("raw_features", "tfidf_vectors"))

    def save(self, path):
        if self.model:
            self.model.write().overwrite().save(path)

    def load(self, path):
        self.model = PipelineModel.load(path)
        return self