from pyspark.ml.classification import LogisticRegression

class LogisticRegressionClassifier:
    def __init__(self, regParam=0.0, elasticNetParam=0.0, maxIter=10):
        self.model = None
        self.maxIter = maxIter
        self.regParam = regParam
        self.elasticNetParam = elasticNetParam

    def fit(self, df, labelCol="tag", featuresCol="word_vectors"):
        lr = LogisticRegression(
            labelCol=labelCol,
            featuresCol=featuresCol,
            maxIter=self.maxIter,
            regParam=self.regParam,
            elasticNetParam=self.elasticNetParam
        )
        self.model = lr.fit(df)
        return self

    def predict(self, df):
        if not self.model:
            raise ValueError("Model must be fitted before transformation.")
        df_transformed = self.model.transform(df)

        return df_transformed