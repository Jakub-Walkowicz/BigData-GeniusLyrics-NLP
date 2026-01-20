from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pandas as pd


class GenreClassifier:
    def __init__(self, regParam=0.0, elasticNetParam=0.0, maxIter=10):
        self.pipeline_model = None
        self.params = {
            "regParam": regParam,
            "elasticNetParam": elasticNetParam,
            "maxIter": maxIter,
        }

    def fit(self, df, labelCol="tag", featuresCols=["word_vectors"]):
        indexer = StringIndexer(
            inputCol=labelCol, outputCol="tag_index", handleInvalid="keep"
        )

        assembler = VectorAssembler(inputCols=featuresCols, outputCol="features")

        lr = LogisticRegression(
            labelCol="tag_index", featuresCol="features", **self.params
        )

        pipeline = Pipeline(stages=[indexer, assembler, lr])
        self.pipeline_model = pipeline.fit(df)

        return self

    def predict(self, df):
        if not self.pipeline_model:
            raise ValueError("Model must be fitted before transformation.")
        df_transformed = self.pipeline_model.transform(df)

        return df_transformed

    def evaluate_all(self, predictions, labelCol="tag"):
        evaluator = MulticlassClassificationEvaluator(
            labelCol=labelCol + "_index", predictionCol="prediction"
        )

        metrics = ["accuracy", "f1", "weightedPrecision", "weightedRecall"]

        result_dict = {
            m: evaluator.setMetricName(m).evaluate(predictions) for m in metrics
        }

        return pd.DataFrame([result_dict])
