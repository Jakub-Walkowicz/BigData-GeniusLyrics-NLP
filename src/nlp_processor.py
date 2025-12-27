from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import ArrayType, StringType
import pl_core_news_sm
import pandas as pd


POLISH_STOPWORDS = [
    "a", "aby", "ach", "acz", "aczkolwiek", "aj", "albo", "ale", "ależ", "ani", "aż", "bardziej", "bardzo", "bez", "bo", "bowiem", "by", "być", "był", "była", "było", "były", "będzie", "będą", "cali", "cała", "cały", "ci", "ciebie", "cię", "co", "cokolwiek", "coś", "czasami", "czasem", "czemu", "czy", "czyli", "daleko", "dla", "dlaczego", "dlatego", "do", "dobrze", "dokąd", "dość", "dużo", "dwa", "dwaj", "dwie", "dwoje", "dziś", "dzisiaj", "gdy", "gdyby", "gdyż", "gdzie", "gdziekolwiek", "gdzieś", "go", "i", "ich", "ile", "im", "inna", "inne", "inny", "innych", "iż", "ja", "jak", "jakaś", "jakby", "jaki", "jakichś", "jakie", "jakiś", "jakiż", "jakkolwiek", "jako", "jakoś", "je", "jeden", "jedna", "jednak", "jednakże", "jedno", "jego", "jej", "jemu", "jest", "jestem", "jeszcze", "jeśli", "jeżeli", "już", "ją", "każdy", "kiedy", "kilka", "kimś", "kto", "ktokolwiek", "ktoś", "która", "które", "którego", "której", "który", "których", "którym", "którzy", "ku", "lecz", "lub", "ma", "mają", "mam", "mi", "między", "mnie", "mną", "może", "można", "mój", "mucha", "my", "mą", "mamy", "na", "nad", "nam", "nami", "nas", "nasz", "nasza", "nasze", "naszego", "naszej", "natomiast", "natychmiast", "nawet", "nią", "nic", "nich", "nie", "niech", "niego", "niej", "niemu", "nigdy", "nim", "nimi", "niż", "no", "o", "obok", "od", "około", "on", "ona", "one", "oni", "ono", "oraz", "oto", "owszem", "po", "pod", "podczas", "pomimo", "ponad", "ponieważ", "po prostu", "potem", "powinien", "powinna", "powinni", "powinno", "poza", "prawie", "przecież", "przed", "przede", "przedtem", "przez", "przy", "roku", "również", "są", "siebie", "siebie", "się", "skąd", "sobie", "sobą", "sposób", "swoje", "ta", "tak", "taki", "takie", "także", "tam", "te", "tego", "tej", "temu", "ten", "teraz", "też", "to", "tobie", "tobą", "toteż", "trzeba", "tu", "tutaj", "twoi", "twoja", "twoje", "twój", "ty", "tych", "tylko", "tym", "u", "w", "wam", "wami", "was", "wasz", "wasza", "wasze", "we", "według", "wiele", "wielu", "więc", "wszystko", "wszyscy", "wszystkie", "wszystkich", "właśnie", "z", "za", "zawsze", "zatem", "że", "żeby",
    "x2", "x3", "x4", "intro", "refren", "zwrotka", "outro", "bridge", "hook"
]

_nlp = None

def get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = pl_core_news_sm.load(disable=["ner", "parser"])
    return _nlp

class NLPProcessor():

    @staticmethod
    def run(df):
        df = NLPProcessor.tokenize(df)
        df = NLPProcessor.remove_stopwords(df)
        df = NLPProcessor.lemmatize(df)
        return df.drop("words_tokens", "words_filtered")

    @staticmethod
    def tokenize(df):
        tokenizer = RegexTokenizer(
            inputCol='lyrics_cleaned',
            outputCol='words_tokens',
            pattern=r'(?U)\w+',
            gaps=False
        )
        return tokenizer.transform(df)

    @staticmethod
    def remove_stopwords(df):
        remover = StopWordsRemover(
            inputCol='words_tokens',
            outputCol='words_filtered',
            stopWords=POLISH_STOPWORDS)
        return remover.transform(df)

    @staticmethod
    @pandas_udf(ArrayType(StringType()))
    def lemmatize_udf(word_list_series: pd.Series) -> pd.Series:
        nlp = get_nlp()
        def process(words):
            if words is None: return []
            doc = nlp(" ".join(words))
            return [t.lemma_.lower() for t in doc if len(t.lemma_) > 2]
        return word_list_series.apply(process)

    @staticmethod
    def lemmatize(df):
        return df.withColumn('words_lemmatized', NLPProcessor.lemmatize_udf(df['words_filtered']))
