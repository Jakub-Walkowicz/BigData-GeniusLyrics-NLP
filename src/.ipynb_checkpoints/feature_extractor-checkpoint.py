from pyspark.ml.feature import Word2Vec
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.functions import vector_to_array

class FeatureExtractor:
    def __init__(self, vectorSize=100, minCount=20):
        self.model = None
        self.vectorSize = vectorSize
        self.minCount = minCount

    def fit(self, df, inputCol="words_lemmatized"):
        word2Vec = Word2Vec(
            vectorSize=self.vectorSize,
            minCount=self.minCount,
            inputCol=inputCol,
            outputCol="word_vectors"
        )

        pipeline = Pipeline(stages=[word2Vec])
        self.model = pipeline.fit(df)
        return self

    def transform(self, df):
        if not self.model:
            raise ValueError("Model must be fitted before transformation.")
        df_transformed = self.model.transform(df)

        return (df_transformed
                .withColumn("feature_array", vector_to_array("word_vectors"))
                .drop("raw_features", "word_vectors"))

    def save(self, path):
        if self.model:
            self.model.write().overwrite().save(path)

    def load(self, path):
        self.model = PipelineModel.load(path)
        return self