# Utilisation d'une image Python avec Java préinstallé
FROM openjdk:11-slim-buster

# Installation des dépendances système et Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    wget \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configuration de Java
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$PATH:$JAVA_HOME/bin

# Installation de Spark
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Copier d'abord requirements.txt pour profiter du cache Docker
WORKDIR /app
COPY requirements.txt .

# Installation des dépendances Python avec pip
RUN pip3 install --no-cache-dir -r requirements.txt

# Téléchargement et installation de Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Copier le reste des fichiers
COPY . .

# Commande de démarrage (sera écrasée par le docker-compose)
CMD ["python3", "producer.py"]