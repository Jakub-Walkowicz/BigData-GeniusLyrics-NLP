# Plan Projektu: Big Data i NLP - Analiza Tekstów Piosenek Genius

## 🚀 Wymagania Technologiczne i Cel

| Etap Projektu | Technologia / Narzędzie | Wymaganie (instr.pdf) |
| :--- | :--- | :--- |
| **I. Przetwarzanie Big Data & NLP** | **PySpark (Python)** | Praca z dużym zbiorem danych (>1 GB), Przetwarzanie danych w środowisku Big Data. |
| **II. Magazyn Cech (Feature Store)** | **Apache Cassandra (NoSQL)** | Wykorzystanie technologii Big Data/NoSQL. |
| **III. Metody Uczenia Maszynowego** | **PySpark MLlib / scikit-learn / Keras** | Wykorzystanie dwóch różnych metod ML. |

---

## 1. PySpark - Pipeline Przetwarzania (I Etap)

Ten etap będzie realizowany w Pythonie z użyciem PySpark i jest kluczowy dla spełnienia wymogu pracy z Big Data.

| Krok | Opis | Moduły PySpark |
| :--- | :--- | :--- |
| **Wczytanie** | Ładowanie pliku CSV (9 GB) do Spark DataFrame. | `pyspark.sql.SparkSession` |
| **Czyszczenie Danych** | Usuwanie metadanych ([Chorus], [Verse]), filtrowanie na język angielski. | `pyspark.sql.functions` |
| **Tokenizacja** | Rozbicie tekstu na słowa. | `pyspark.ml.feature.Tokenizer` |
| **Normalizacja/Cechy** | Usunięcie *stop words*, a następnie wektoryzacja tekstu (np. TF-IDF). | `pyspark.ml.feature.StopWordsRemover`, `HashingTF`, `IDF` |

---

## 2. Apache Cassandra - Magazyn Cech (II Etap)

Po przetworzeniu przez Sparka, gotowe wektory cech (numeryczne reprezentacje tekstów) oraz etykiety zostaną zapisane do Cassandry.

* **Zastosowanie:** Cassandra będzie działać jako magazyn danych gotowych do trenowania (Feature Store), co jest formalnym wykorzystaniem bazy NoSQL.
* **Integracja:** Użycie konektora Spark-Cassandra do zapisu Spark DataFrame do tabeli w Cassandrze.

---

## 3. Metody ML (III Etap)

Minimalne wymaganie to dwie metody ML, plan obejmuje trzy:

| Metoda ML | Rodzaj Problemu | Cel Projektu | Metryka Jakości |
| :--- | :--- | :--- | :--- |
| **1. Klasyfikacja** | Nadzorowana | Klasyfikacja Gatunku (np. Random Forest). | Accuracy, Precision/Recall, F1-Score. |
| **2. Regresja** | Nadzorowana | Przewidywanie Popularności (`views`) (np. XGBoost). | RMSE (Root Mean Squared Error). |
| **3. Generowanie Tekstu** | Nienadzorowana / Generatywna | **(Rozszerzenie)** Generowanie Tekstów Piosenek (np. LSTM / Keras). | - |