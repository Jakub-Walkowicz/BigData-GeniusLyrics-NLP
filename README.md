# Projekt zaliczeniowy "Genius Lyrics Big Data" z przedmiotu "Systemy BigData i NoSQL"

## Spis Treści
1. [Wprowadzenie](#wprowadzenie)
2. [Instrukcja uruchomienia](#instrukcja-uruchomienia)
3. [Architektura i technologie](#architektura-i-technologie)
4. [Opis modułów](#opis-modułów)
5. [Przepływ pracy (Notebooki)](#przepływ-pracy-notebooki)
6. [Wyniki i ewaluacja](#wyniki-i-ewaluacja)
7. [Napotkane trudności i proponowane udoskonalenia](#napotkane-trudności-i-proponowane-udoskonalenia)

## Wprowadzenie
Projekt ma na celu analizę i przetwarzanie dużych zbiorów danych tekstowych (teksty piosenek z serwisu Genius) przy użyciu technologii Big Data. Głównym celem jest przygotowanie danych, ekstrakcja cech z tekstu oraz budowa modeli uczenia maszynowego do klasyfikacji gatunków muzycznych i predykcji popularności utworów.

Pochodzenie danych: https://www.kaggle.com/datasets/carlosgdcj/genius-song-lyrics-with-language-information/data

## Instrukcja uruchomienia

1. W wybranym katalogu na komputerze wykonać komendę: `git clone <adres_repozytorium>` oraz `cd <nazwa_katalogu_projektu>`.
2. Plik .csv (źródło powyżej) należy umieścić w katalogu `data/` wewnątrz katalogu projektu.
3. W głównym katalogu projektu uruchamiamy komendę `docker compose up -d --build`
4. Notebooki uruchamiamy z poziomu UI JupyterLab `http://localhost:8888` w oznaczonej kolejności.

## Architektura i Technologie
- **Apache Spark**: Główny silnik przetwarzania danych (PySpark).
- **Apache Cassandra**: Baza danych NoSQL do przechowywania przetworzonych rekordów.
- **Spacy**: Biblioteka NLP wykorzystywana do lematyzacji (model `pl_core_news_sm`).
- **Spark MLlib**: Biblioteka do uczenia maszynowego (Word2Vec, Regresja Logistyczna, Las Losowy).

## Opis Modułów

### 1. Konfiguracja i Dostęp do Danych

#### `src/spark/spark_config.py`
Zarządza konfiguracją sesji Spark.
- **`create_spark_session()`**: Inicjalizuje `SparkSession` z ustawieniami dla klastra (`spark://spark-master:7077`) oraz konektora Cassandra (`com.datastax.spark:spark-cassandra-connector`). Konfiguruje pamięć dla executorów (3g) i drivera (1500m).

#### `src/data_loader.py`
Odpowiada za wczytywanie danych źródłowych.
- **`DataLoader`**: Klasa obsługująca odczyt pliku CSV (`song_lyrics.csv`).

#### `src/repository/cassandra_provider.py`
Warstwa persystencji danych.
- **`CassandraProvider`**:
  - **`save(df)`**: Zapisuje DataFrame do tabeli `processed_songs` w keyspace `genius_space`. Konwertuje wektory ML (`word_vectors`) na tablice, aby były kompatybilne z Cassandrą.
  - **`load(spark)`**: Wczytuje dane z Cassandry i przywraca kolumnę `word_vectors` do formatu wektorowego Spark ML.

### 2. Przetwarzanie i Analiza (ETL & EDA)

#### `src/eda_analyser.py`
Narzędzie do eksploracyjnej analizy danych.
- **`EdaAnalyser`**:
  - **`run_full_eda_report(columns)`**: Generuje raport zawierający podstawowe statystyki, liczbę wartości null oraz najpopularniejsze piosenki (dla języka polskiego i angielskiego).
  - Metody pomocnicze: `_print_basic_report`, `_print_null_report`, `_print_top_songs`.

#### `src/preprocessor.py`
Wstępne czyszczenie danych.
- **`Preprocessor`**:
  - **`run(df)`**: Usuwa rekordy z brakującymi danymi, filtruje utwory w języku polskim (`language == 'pl'`) i czyści kolumnę `lyrics`.
  - **`_clean_text_logic`**: Usuwa metadane (np. `[Chorus]`), znaki interpunkcyjne i nadmiarowe spacje z tekstu piosenek.

#### `src/nlp_processor.py`
Przetwarzanie języka naturalnego.
- **`NLPProcessor`**:
  - **`run(df)`**: Wykonuje potok NLP: Tokenizacja -> Usuwanie Stopwords -> Lematyzacja.
  - **`tokenize`**: Dzieli tekst na słowa przy użyciu

## Przepływ Pracy (Notebooki)

#### `notebooks/01_feature_engineering_and_storage.ipynb`
Notebook odpowiedzialny za inżynierię cech i zapis danych.
1. **Inicjalizacja**: Utworzenie sesji Spark.
2. **Wczytanie danych**: Pobranie surowych danych z CSV.
3. **EDA**: Analiza wstępna (statystyki, braki, top utwory).
4. **Preprocessing**: Czyszczenie tekstu i filtracja języka (tylko PL).
5. **NLP**: Tokenizacja, usuwanie stopwords, lematyzacja.
6. **Feature Extraction**: Trening Word2Vec i transformacja danych.
7. **Zapis**: Eksport przetworzonych danych do tabeli w Cassandra.

**Czas uruchomienia**: ok. 50 min

#### `notebooks/02_model_training_and_evaluation.ipynb`
Notebook odpowiedzialny za trenowanie modeli.
1. **Wczytanie danych**: Pobranie przetworzonych rekordów z Cassandry.
2. **Podział danych**: Train/Test split (80/20).
3. **Klasyfikacja Gatunku**:
   - Trening `GenreClassifier` (Regresja Logistyczna).
   - Ewaluacja wyników (Accuracy, F1).
   - Analiza przykładowych predykcji.
4. **Predykcja Wyświetleń**:
   - Inżynieria cech (logarytmizacja, liczba piosenek wykonawcy).
   - Trening `ViewsPredictor` (Las Losowy).
   - Ewaluacja wyników (RMSE, R2).
   - Analiza przykładowych predykcji.

**Czas uruchomienia**: ok. 1 - 2 min

## Wyniki i ewaluacja

### 5.1. Charakterystyka Zbioru (Top utwory PL)
| Tytuł | Artysta | Wyświetlenia |
| :--- | :--- | :--- |
| Pan Tadeusz - Inwokacja | Adam Mickiewicz | 1 865 798 [cite: 108, 111] |
| Tamagotchi | TACONAFIDE | 618 358 [cite: 109, 112] |
| Half dead | Quebonafide | 484 043 [cite: 110, 113] |

### 5.2. Modele Uczenia Maszynowego

#### Klasyfikacja Gatunków (Logistic Regression)
Model klasyfikuje utwory na podstawie zwektoryzowanego tesktu piosenek `word_vectors`.
* **Accuracy**: 0.8148
* **F1 Score**: 0.7756
* **Weighted Precision**: 0.7764

#### Predykcja Popularności (Random Forest Regressor)
Model przewiduje zlogarytmowaną liczbę wyświetleń, wykorzystując zwektoryzowane teskty piosenek `word_vectors`, gatunek muzyki `tag` oraz liczbę utworów wykonawcy (`artist_song_count`).
* **RMSE**: 1.6934
* **R2**: 0.3598

**Transformacja zmiennej celu**: W procesie modelowania zdecydowano się na wykorzystanie zlogarytmowanej liczby wyświetleń. Decyzja ta wynikała z faktu, że surowa liczba wyświetleń charakteryzuje się silnie prawostronnym rozkładem skośnym (long tail) – nieliczne „hity” osiągają ekstremalnie wysokie wartości, podczas gdy większość utworów notuje znacznie mniejszą popularność.

## Napotkane trudności i proponowane udoskonalenia

### Trudności z cechą "artist"
Podczas modelowania predykcji popularności, cecha `artist` stwarzała problemy techniczne. Bardzo duża liczba unikalnych artystów w połączeniu z małą liczbą utworów dla większości z nich uniemożliwiała skuteczne zastosowanie One-Hot Encodingu ze względu na generowanie ekstremalnie rzadkich macierzy i przekleństwo wymiarowości.

**Zastosowane rozwiązanie:** Zamiast bezpośredniego użycia identyfikatora artysty, wprowadzono nową cechę: liczbę utworów wykonawcy (`artist_song_count`). Podejście to pozwoliło na skuteczną agregację informacji o popularności bez rozszerzania wymiarowości danych.

### Proponowane udoskonalenia
* **Generator piosenek**: Rozbudowa projektu o moduł generatywny (np. model LSTM lub Transformer), który na bazie istniejących wektorów słów i stylu danego artysty byłby w stanie tworzyć autorskie teksty piosenek.
* **Analiza sentymentu**: Dodanie analizy emocjonalnej tekstu jako dodatkowej cechy wejściowej do predykcji popularności.
