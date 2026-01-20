from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
import pandas as pd

class ViewsPredictor():
    @staticmethod
    def logarithmise_label_and_get_avg_views(train, test, label="views"):
        label_log = label+"_log"
        train = train.withColumn(label_log, F.log1p(F.col(label)))
        test = test.withColumn(label_log, F.log1p(F.col(label)))
        
        avg_views = train.groupby("artist") \
            .agg(F.avg(F.col(label_log))
            .alias("artist_avg_views"))
        global_mean = train.select(F.avg(F.col(label_log))).first()[0]
        
        train = train.join(avg_views, on="artist", how="left") \
            .fillna(global_mean, ["artist_avg_views"])
        test = test.join(avg_views, on="artist", how="left") \
            .fillna(global_mean, ["artist_avg_views"])
        
        return train, test
              
    def __init__(self, maxDepth=10, maxBins=32, numTrees=20):
        self.pipeline_model = None
        self.params = {
            "maxDepth": maxDepth,
            "maxBins": maxBins,
            "numTrees": numTrees
        }
        
    def fit(self, df, labelCol="views_log"):
        categorical_cols = ["tag"]
        numerical_cols = ["word_vectors", "artist_avg_views"]
        indexed_cols = [c+"_index" for c in categorical_cols]
        
        indexer = StringIndexer(
            inputCols=categorical_cols,
            outputCols=indexed_cols,
            handleInvalid="keep"
        )

        assembler = VectorAssembler(
            inputCols=numerical_cols+indexed_cols,
            outputCol="features"
        )
        
        rfr = RandomForestRegressor(
            featuresCol="features",
            labelCol=labelCol,
            **self.params
        )
        
        pipeline = Pipeline(stages=[indexer, assembler, rfr])
        self.pipeline_model = pipeline.fit(df)
        return self
    
    def predict(self, df):
        if not self.pipeline_model:
            raise ValueError("Model must be fitted before transformation.")
        df_transformed = self.pipeline_model.transform(df)

        return df_transformed
    
    def evaluate_all(self, predictions, labelCol="views_log"):
        evaluator = RegressionEvaluator(
            labelCol=labelCol,
            predictionCol="prediction"
        )
        
        metrics = ["rmse", "r2"]

        result_dict = {m: evaluator.setMetricName(m).evaluate(predictions) for m in metrics}
        
        return pd.DataFrame([result_dict])
