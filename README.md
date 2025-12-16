## Data read and processing: Apache PySpark 


## DB: Apache Cassandra
Used to store NLP results.

ML 1: Klasyfikacja GatunkuKlasyfikacja (nadzorowana)Random Forest ClassifierPrzewidywanie gatunku (tag) piosenki na podstawie jej tekstu (wektora cech). Metryki: Accuracy, Precision, Recall, F1-Score.ML 2: Predykcja PopularnościRegresja (nadzorowana)XGBoost Regressor (lub Regresja Liniowa)Przewidywanie logarytmu liczby wyświetleń (views) na podstawie cech lingwistycznych i metadanych. Metryka: RMSE (Root Mean Squared Error).\

Możliwe Rozszerzenia 

Proponowane	Generatywny Model Językowy (LSTM/GPT)