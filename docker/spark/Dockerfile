FROM apache/spark-py:v3.4.0

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/work-dir

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt