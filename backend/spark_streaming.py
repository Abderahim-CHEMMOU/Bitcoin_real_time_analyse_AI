from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import time
from py4j.java_gateway import java_import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("BitcoinDataProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

def wait_for_namenode(spark):
    """Attend que le NameNode soit prêt et crée les répertoires nécessaires"""
    max_retries = 10
    retry_interval = 5
    
    # Obtenir le contexte Hadoop FileSystem
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    
    for i in range(max_retries):
        try:
            logger.info(f"Tentative {i+1}/{max_retries} de connexion au NameNode...")
            
            # Création des répertoires avec l'API Java
            paths = [
                '/bitcoin/raw_data',
                '/bitcoin/hourly_metrics',
                '/bitcoin/checkpoints'
            ]
            
            for path in paths:
                hdfs_path = spark._jvm.Path(path)
                if not fs.exists(hdfs_path):
                    fs.mkdirs(hdfs_path)
                    logger.info(f"Répertoire créé : {path}")
            
            logger.info("NameNode est prêt et les répertoires sont créés!")
            return True
            
        except Exception as e:
            logger.warning(f"NameNode n'est pas encore prêt: {str(e)}")
            if i < max_retries - 1:
                logger.info(f"Nouvelle tentative dans {retry_interval} secondes...")
                time.sleep(retry_interval)
    
    return False

def define_schema():
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("bid_price", DoubleType(), True),
        StructField("ask_price", DoubleType(), True),
        StructField("bid_qty", DoubleType(), True),
        StructField("ask_qty", DoubleType(), True),
        StructField("volume_24h", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("trade_timestamp", LongType(), True)
    ])

def process_batch(df, epoch_id):
    try:
        # Calculer les métriques agrégées par heure
        hourly_metrics = df \
            .groupBy("date", "hour") \
            .agg(
                avg("mid_price").alias("avg_price"),
                avg("spread").alias("avg_spread"),
                avg("spread_percentage").alias("avg_spread_percentage"),
                sum("total_liquidity").alias("total_liquidity"),
                avg("price_volatility").alias("avg_volatility"),
                max("mid_price").alias("high_price"),
                min("mid_price").alias("low_price"),
                avg("volume_24h").alias("avg_volume"),
                last("market_cap").alias("last_market_cap")
            )

        # Sauvegarder les métriques
        hourly_metrics.write \
            .mode("append") \
            .parquet("hdfs://namenode:9000/bitcoin/hourly_metrics")

        logger.info(f"Batch {epoch_id} traité avec succès")
        logger.info("Métriques calculées :")
        hourly_metrics.show(truncate=False)
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement du batch {epoch_id}: {str(e)}")

def start_streaming():
    spark = create_spark_session()

    # Attendre que le NameNode soit prêt
    if not wait_for_namenode(spark):
        logger.error("Impossible de se connecter au NameNode après plusieurs tentatives")
        return

    schema = define_schema()

    # Lecture du flux Kafka
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "cryptoTopic") \
        .option("startingOffsets", "latest") \
        .load()

    # Parsing et transformation des données
    parsed_df = df_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Ajout des colonnes calculées
    transformed_df = parsed_df \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .withColumn("date", to_date("timestamp")) \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("spread", col("ask_price") - col("bid_price")) \
        .withColumn("mid_price", (col("ask_price") + col("bid_price")) / 2) \
        .withColumn("spread_percentage", (col("spread") / col("mid_price")) * 100) \
        .withColumn("total_liquidity", col("bid_qty") + col("ask_qty")) \
        .withColumn("price_volatility", (col("high_24h") - col("low_24h")) / col("mid_price") * 100)

    # Écriture des données brutes
    query_raw = transformed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/bitcoin/raw_data") \
        .option("checkpointLocation", "hdfs://namenode:9000/bitcoin/checkpoints/raw") \
        .start()

    # Écriture des métriques agrégées
    query_metrics = transformed_df \
        .writeStream \
        .trigger(processingTime='1 minute') \
        .foreachBatch(process_batch) \
        .start()

    # Attendre que les requêtes se terminent
    query_raw.awaitTermination()
    query_metrics.awaitTermination()

if __name__ == "__main__":
    logger.info("Démarrage du job Spark Streaming...")
    start_streaming()