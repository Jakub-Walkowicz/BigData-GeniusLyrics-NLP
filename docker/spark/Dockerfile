FROM apache/spark-py:latest

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    pyspark==3.5.0 \
    spacy \
    numpy \
    jupyterlab \
    cassandra-driver

RUN pip install https://github.com/explosion/spacy-models/releases/download/pl_core_news_sm-3.7.0/pl_core_news_sm-3.7.0-py3-none-any.whl

WORKDIR /opt/spark/work-dir
COPY src/ /opt/spark/work-dir/src

RUN chown -R 185:185 /opt/spark/work-dir

USER 185