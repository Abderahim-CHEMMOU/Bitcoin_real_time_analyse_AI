# Utiliser une image avec Java et Python préinstallés
FROM jupyter/pyspark-notebook:spark-3.3.0

USER root

# Installation des dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    libopenblas-dev \
    liblapack-dev \
    gfortran \
    pkg-config \
    libfreetype6-dev \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configuration de l'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Configuration Spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

WORKDIR /app

# Copier requirements.txt
COPY requirements.txt .

# Installation des dépendances Python
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt

# Vérifier les installations
RUN which java && \
    java -version && \
    python3 -c "import pyspark; print('PySpark version:', pyspark.__version__)"

# Copier le reste des fichiers
COPY . .

USER $NB_UID

CMD ["python3", "producer.py"]