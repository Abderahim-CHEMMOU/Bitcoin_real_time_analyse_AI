import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
import matplotlib.pyplot as plt
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitcoinPredictor:
    def __init__(self, sequence_length=60):
        self.sequence_length = sequence_length
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model = None
        self.spark = SparkSession.builder \
            .appName("BitcoinPrediction") \
            .getOrCreate()

    def load_data_from_hdfs(self):
        try:
            df = self.spark.read.parquet("hdfs://namenode:9000/bitcoin/hourly_metrics")
            df = df.toPandas()
            df['datetime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'], unit='h')
            df = df.sort_values('datetime')
            return df
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {str(e)}")
            raise

    def save_predictions_to_hdfs(self, y_test, y_pred, timestamps):
        """Sauvegarde les prédictions dans HDFS"""
        try:
            # Création du DataFrame avec les prédictions
            predictions_df = pd.DataFrame({
                'timestamp': timestamps,
                'actual_price': y_test,
                'predicted_price': y_pred,
                'prediction_error': np.abs(y_test - y_pred),
                'error_percentage': np.abs((y_test - y_pred) / y_test) * 100
            })

            # Conversion en Spark DataFrame
            schema = StructType([
                StructField("timestamp", TimestampType(), True),
                StructField("actual_price", DoubleType(), True),
                StructField("predicted_price", DoubleType(), True),
                StructField("prediction_error", DoubleType(), True),
                StructField("error_percentage", DoubleType(), True)
            ])

            spark_df = self.spark.createDataFrame(predictions_df, schema)

            # Sauvegarde dans HDFS
            spark_df.write \
                .mode("append") \
                .partitionBy("timestamp") \
                .parquet("hdfs://namenode:9000/bitcoin/predictions")

            logger.info("Prédictions sauvegardées dans HDFS avec succès")

            # Sauvegarde des métriques globales
            metrics_df = pd.DataFrame([{
                'timestamp': datetime.now(),
                'mse': np.mean((y_test - y_pred) ** 2),
                'rmse': np.sqrt(np.mean((y_test - y_pred) ** 2)),
                'mae': np.mean(np.abs(y_test - y_pred)),
                'mean_error_percentage': np.mean(np.abs((y_test - y_pred) / y_test) * 100)
            }])

            metrics_schema = StructType([
                StructField("timestamp", TimestampType(), True),
                StructField("mse", DoubleType(), True),
                StructField("rmse", DoubleType(), True),
                StructField("mae", DoubleType(), True),
                StructField("mean_error_percentage", DoubleType(), True)
            ])

            spark_metrics_df = self.spark.createDataFrame(metrics_df, metrics_schema)

            spark_metrics_df.write \
                .mode("append") \
                .parquet("hdfs://namenode:9000/bitcoin/prediction_metrics")

            logger.info("Métriques de prédiction sauvegardées dans HDFS avec succès")

        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des prédictions: {str(e)}")
            raise

    def save_future_predictions(self, predictions, start_time):
        """Sauvegarde les prédictions futures dans HDFS"""
        try:
            # Création des timestamps pour les prédictions futures
            timestamps = [start_time + timedelta(hours=i) for i in range(len(predictions))]
            
            future_df = pd.DataFrame({
                'timestamp': timestamps,
                'predicted_price': predictions,
                'is_future': True
            })

            schema = StructType([
                StructField("timestamp", TimestampType(), True),
                StructField("predicted_price", DoubleType(), True),
                StructField("is_future", BooleanType(), True)
            ])

            spark_df = self.spark.createDataFrame(future_df, schema)

            spark_df.write \
                .mode("append") \
                .partitionBy("timestamp") \
                .parquet("hdfs://namenode:9000/bitcoin/future_predictions")

            logger.info("Prédictions futures sauvegardées dans HDFS avec succès")

        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des prédictions futures: {str(e)}")
            raise

    def prepare_data(self, df):
        """Prépare les données pour l'entraînement"""
        features = ['avg_price', 'avg_spread', 'total_liquidity', 'avg_volatility', 'avg_volume']
        scaled_data = self.scaler.fit_transform(df[features])
        
        X, y, timestamps = [], [], []
        for i in range(len(scaled_data) - self.sequence_length):
            X.append(scaled_data[i:(i + self.sequence_length)])
            y.append(scaled_data[i + self.sequence_length, 0])
            timestamps.append(df['datetime'].iloc[i + self.sequence_length])
            
        X = np.array(X)
        y = np.array(y)
        
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]
        timestamps_test = timestamps[train_size:]
        
        return (X_train, y_train), (X_test, y_test, timestamps_test), features

    def create_model(self, input_shape):
        """Crée le modèle LSTM"""
        model = Sequential([
            LSTM(100, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),
            LSTM(50, return_sequences=False),
            Dropout(0.2),
            Dense(25),
            Dense(1)
        ])
        
        model.compile(
            optimizer=Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae']
        )
        return model

    def train_model(self, X_train, y_train, X_test, y_test):
        """Entraîne le modèle"""
        self.model = self.create_model(input_shape=(X_train.shape[1], X_train.shape[2]))
        
        history = self.model.fit(
            X_train, y_train,
            validation_data=(X_test, y_test),
            epochs=50,
            batch_size=32,
            verbose=1
        )
        return history

    def predict_future(self, last_sequence, n_future=24):
        """Prédit les n prochaines heures"""
        current_sequence = last_sequence.copy()
        future_predictions = []

        for _ in range(n_future):
            # Prédire le prochain prix
            next_pred = self.model.predict(current_sequence.reshape(1, self.sequence_length, -1))
            future_predictions.append(next_pred[0, 0])
            
            # Mettre à jour la séquence pour la prochaine prédiction
            current_sequence = np.roll(current_sequence, -1, axis=0)
            current_sequence[-1] = next_pred

        # Inverse la normalisation pour obtenir les vrais prix
        future_pred_inv = self.scaler.inverse_transform(
            np.zeros((len(future_predictions), self.scaler.n_features_)))
        future_pred_inv[:, 0] = future_predictions
        
        return future_pred_inv[:, 0]

    def run_pipeline(self):
        """Exécute le pipeline complet"""
        try:
            logger.info("Chargement des données...")
            df = self.load_data_from_hdfs()
            
            logger.info("Préparation des données...")
            (X_train, y_train), (X_test, y_test, timestamps_test), features = self.prepare_data(df)
            
            logger.info("Entraînement du modèle...")
            history = self.train_model(X_train, y_train, X_test, y_test)
            
            # Prédictions sur l'ensemble de test
            y_pred = self.model.predict(X_test)
            
            # Inverse la normalisation
            y_test_inv = self.scaler.inverse_transform(np.zeros((len(y_test), self.scaler.n_features_)))
            y_test_inv[:, 0] = y_test
            y_test_price = y_test_inv[:, 0]
            
            y_pred_inv = self.scaler.inverse_transform(np.zeros((len(y_pred), self.scaler.n_features_)))
            y_pred_inv[:, 0] = y_pred[:, 0]
            y_pred_price = y_pred_inv[:, 0]
            
            logger.info("Sauvegarde des prédictions historiques...")
            self.save_predictions_to_hdfs(y_test_price, y_pred_price, timestamps_test)
            
            # Prédictions futures
            last_sequence = X_test[-1]
            future_predictions = self.predict_future(last_sequence)
            
            logger.info("Sauvegarde des prédictions futures...")
            last_timestamp = timestamps_test[-1]
            self.save_future_predictions(future_predictions, last_timestamp)
            
            logger.info("Pipeline terminé avec succès")
            
        except Exception as e:
            logger.error(f"Erreur dans le pipeline: {str(e)}")
            raise

if __name__ == "__main__":
    predictor = BitcoinPredictor(sequence_length=60)
    predictor.run_pipeline()