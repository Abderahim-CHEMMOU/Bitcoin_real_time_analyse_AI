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
import traceback
import time
from py4j.java_gateway import java_import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitcoinPredictor:
    def __init__(self, sequence_length=24):  # Réduit à 24 heures
        self.sequence_length = sequence_length
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model = None
        self.spark = SparkSession.builder \
            .appName("BitcoinPrediction") \
            .getOrCreate()
        
        # Créer les répertoires HDFS nécessaires
        self.wait_for_namenode()

    def wait_for_namenode(self):
        """Attend que le NameNode soit prêt et sorte du safe mode, puis crée les répertoires nécessaires"""
        max_retries = 15
        retry_interval = 10  # 10 secondes
        
        # Obtenir le contexte Hadoop FileSystem
        java_import(self.spark._jvm, 'org.apache.hadoop.fs.Path')
        java_import(self.spark._jvm, 'org.apache.hadoop.hdfs.DFSUtil')
        java_import(self.spark._jvm, 'org.apache.hadoop.hdfs.DFSConfigKeys')
        java_import(self.spark._jvm, 'org.apache.hadoop.hdfs.HdfsConfiguration')
        
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        
        for i in range(max_retries):
            try:
                logger.info(f"Tentative {i+1}/{max_retries} de connexion au NameNode...")
                
                # Vérifier si le NameNode est en safe mode
                safemode_check_cmd = "hdfs dfsadmin -safemode get"
                import subprocess
                result = subprocess.run(safemode_check_cmd, shell=True, capture_output=True, text=True)
                
                if "Safe mode is ON" in result.stdout:
                    logger.warning("NameNode est en mode sécurité (safe mode), attente...")
                    time.sleep(retry_interval)
                    continue
                
                # Création des répertoires avec l'API Java
                paths = [
                    '/bitcoin/raw_data',
                    '/bitcoin/hourly_metrics',
                    '/bitcoin/checkpoints',
                    '/bitcoin/predictions',
                    '/bitcoin/prediction_metrics',
                    '/bitcoin/future_predictions'
                ]
                
                for path in paths:
                    hdfs_path = self.spark._jvm.Path(path)
                    if not fs.exists(hdfs_path):
                        fs.mkdirs(hdfs_path)
                        logger.info(f"Répertoire créé : {path}")
                    else:
                        logger.info(f"Répertoire existant : {path}")
                
                logger.info("NameNode est prêt et les répertoires sont créés!")
                return True
                
            except Exception as e:
                logger.warning(f"NameNode n'est pas encore prêt: {str(e)}")
                if i < max_retries - 1:
                    logger.info(f"Nouvelle tentative dans {retry_interval} secondes...")
                    time.sleep(retry_interval)
        
        logger.error("Impossible de se connecter au NameNode après plusieurs tentatives")
        return False

    def save_with_retry(self, df, path, mode="append", partitionBy=None, max_retries=5, retry_interval=10):
        """Sauvegarde les données dans HDFS avec mécanisme de réessai"""
        for attempt in range(max_retries):
            try:
                writer = df.write.mode(mode)
                if partitionBy:
                    writer = writer.partitionBy(partitionBy)
                writer.parquet(path)
                logger.info(f"Données sauvegardées avec succès dans {path}")
                return True
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"Tentative {attempt+1}/{max_retries} - Erreur lors de la sauvegarde: {str(e)}")
                
                if "safe mode" in error_str and attempt < max_retries - 1:
                    logger.info(f"NameNode en safe mode, attente de {retry_interval} secondes...")
                    time.sleep(retry_interval)
                elif attempt < max_retries - 1:
                    logger.info(f"Réessai dans {retry_interval} secondes...")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Échec de la sauvegarde après {max_retries} tentatives")
                    raise

    def load_data_from_hdfs(self):
        try:
            # Lecture des données depuis HDFS
            df = self.spark.read.parquet("hdfs://namenode:9000/bitcoin/hourly_metrics")
            logger.info(f"Données chargées depuis HDFS: {df.count()} enregistrements")
            
            # Affichage du schéma pour le débogage
            logger.info("Schéma des données:")
            df.printSchema()
            
            # Affichage des premières lignes pour le débogage
            logger.info("Échantillon des données:")
            df.show(5, truncate=False)
            
            # Conversion en DataFrame pandas
            df = df.toPandas()
            
            # Vérification des colonnes disponibles
            logger.info(f"Colonnes disponibles: {df.columns.tolist()}")
            
            # Création de la colonne datetime
            if 'date' in df.columns and 'hour' in df.columns:
                df['datetime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'], unit='h')
                df = df.sort_values('datetime')
            else:
                raise ValueError("Les colonnes 'date' et 'hour' sont requises mais non disponibles")
            
            return df
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {str(e)}")
            logger.error(traceback.format_exc())
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

            # Sauvegarde dans HDFS avec réessais
            self.save_with_retry(
                spark_df, 
                "hdfs://namenode:9000/bitcoin/predictions", 
                mode="append", 
                partitionBy="timestamp"
            )

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

            # Sauvegarde dans HDFS avec réessais
            self.save_with_retry(
                spark_metrics_df, 
                "hdfs://namenode:9000/bitcoin/prediction_metrics", 
                mode="append"
            )

            logger.info("Métriques de prédiction sauvegardées dans HDFS avec succès")

        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des prédictions: {str(e)}")
            logger.error(traceback.format_exc())
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

            # Sauvegarde dans HDFS avec réessais
            self.save_with_retry(
                spark_df, 
                "hdfs://namenode:9000/bitcoin/future_predictions", 
                mode="append", 
                partitionBy="timestamp"
            )

            logger.info("Prédictions futures sauvegardées dans HDFS avec succès")

        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des prédictions futures: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def prepare_data(self, df):
        """Prépare les données pour l'entraînement"""
        try:
            # Définir les caractéristiques (features) à utiliser pour la prédiction
            features = ['avg_price', 'avg_spread', 'avg_spread_percentage', 
                      'total_liquidity', 'avg_volatility', 'high_price', 
                      'low_price', 'avg_volume']
            
            # Vérifiez si toutes les colonnes nécessaires existent
            missing_features = [f for f in features if f not in df.columns]
            if missing_features:
                logger.warning(f"Colonnes manquantes dans les données: {missing_features}")
                logger.info(f"Colonnes disponibles: {df.columns.tolist()}")
                
                # Utiliser seulement les colonnes disponibles
                features = [f for f in features if f in df.columns]
                logger.info(f"Utilisation des colonnes disponibles: {features}")
                
                if not features:
                    raise ValueError("Aucune colonne utilisable trouvée dans les données")
            
            logger.info(f"Shape des données avant scaling: {df[features].shape}")
            
            # Vérifiez s'il y a des valeurs NaN et remplacez-les
            if df[features].isna().any().any():
                logger.warning("Valeurs NaN détectées, elles seront remplacées")
                df[features] = df[features].fillna(method='ffill')
                # Si encore des NaN au début, les remplacer par 0
                df[features] = df[features].fillna(0)
            
            # Normalisation des données
            scaled_data = self.scaler.fit_transform(df[features])
            logger.info(f"Shape des données après scaling: {scaled_data.shape}")
            
            # Vérifier si nous avons assez de données
            if len(scaled_data) <= self.sequence_length:
                logger.error(f"Pas assez de données: {len(scaled_data)} points < {self.sequence_length}")
                # Ajuster la longueur de séquence si nécessaire
                self.sequence_length = max(1, len(scaled_data) // 4)
                logger.info(f"Ajustement de sequence_length à {self.sequence_length}")
            
            # Préparation des séquences X, y
            X, y, timestamps = [], [], []
            
            for i in range(len(scaled_data) - self.sequence_length):
                X.append(scaled_data[i:(i + self.sequence_length)])
                y.append(scaled_data[i + self.sequence_length, 0])  # Index 0 correspond à avg_price
                timestamps.append(df['datetime'].iloc[i + self.sequence_length])
            
            X = np.array(X)
            y = np.array(y)
            
            logger.info(f"Shape de X: {X.shape}, shape de y: {y.shape}")
            
            # Vérifier si nous avons suffisamment de séquences
            if len(X) < 10:
                logger.error(f"Pas assez de séquences: {len(X)} < 10")
                raise ValueError("Pas assez de données pour l'entraînement")
            
            # Division train/test
            train_size = int(len(X) * 0.8)
            X_train, X_test = X[:train_size], X[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]
            timestamps_test = timestamps[train_size:]
            
            logger.info(f"Dimensions - X_train: {X_train.shape}, X_test: {X_test.shape}")
            
            return (X_train, y_train), (X_test, y_test, timestamps_test), features
            
        except Exception as e:
            logger.error(f"Erreur lors de la préparation des données: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def create_model(self, input_shape):
        """Crée le modèle LSTM"""
        try:
            logger.info(f"Création du modèle avec input_shape: {input_shape}")
            
            model = Sequential([
                LSTM(50, return_sequences=True, input_shape=input_shape),
                Dropout(0.2),
                LSTM(25, return_sequences=False),
                Dropout(0.2),
                Dense(15),
                Dense(1)
            ])
            
            model.compile(
                optimizer=Adam(learning_rate=0.001),
                loss='mse',
                metrics=['mae']
            )
            
            # Afficher un résumé du modèle pour vérification
            model.summary(print_fn=logger.info)
            
            return model
            
        except Exception as e:
            logger.error(f"Erreur lors de la création du modèle: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def train_model(self, X_train, y_train, X_test, y_test):
        """Entraîne le modèle"""
        try:
            # Vérifier la forme des données
            logger.info(f"X_train shape: {X_train.shape}")
            
            if len(X_train.shape) < 3:
                logger.error(f"X_train doit avoir 3 dimensions, mais a {len(X_train.shape)} dimensions")
                # Reshape si nécessaire - pour le cas où il n'y a qu'une seule feature
                if len(X_train.shape) == 2:
                    X_train = X_train.reshape(X_train.shape[0], X_train.shape[1], 1)
                    X_test = X_test.reshape(X_test.shape[0], X_test.shape[1], 1)
                    logger.info(f"Reshape effectué. Nouvelle X_train shape: {X_train.shape}")
            
            # Créer le modèle avec la bonne forme d'entrée
            input_shape = (X_train.shape[1], X_train.shape[2])
            self.model = self.create_model(input_shape)
            
            # Configuration de l'entraînement avec early stopping
            from tensorflow.keras.callbacks import EarlyStopping
            early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
            
            # Entraînement du modèle
            history = self.model.fit(
                X_train, y_train,
                validation_data=(X_test, y_test),
                epochs=50,
                batch_size=32,
                callbacks=[early_stopping],
                verbose=1
            )
            
            return history
            
        except Exception as e:
            logger.error(f"Erreur lors de l'entraînement du modèle: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def predict_future(self, last_sequence, n_future=24):
        """Prédit les n prochaines heures"""
        try:
            logger.info(f"Prédiction des {n_future} prochaines heures")
            logger.info(f"Forme de la dernière séquence: {last_sequence.shape}")
            
            current_sequence = last_sequence.copy()
            future_predictions = []

            for i in range(n_future):
                # S'assurer que la forme est correcte pour la prédiction
                if len(current_sequence.shape) == 2:
                    prediction_input = current_sequence.reshape(1, current_sequence.shape[0], current_sequence.shape[1])
                else:
                    prediction_input = current_sequence.reshape(1, self.sequence_length, -1)
                
                # Prédire le prochain prix
                next_pred = self.model.predict(prediction_input)
                logger.info(f"Prédiction {i+1}/{n_future}: {next_pred[0, 0]}")
                future_predictions.append(next_pred[0, 0])
                
                # Mettre à jour la séquence pour la prochaine prédiction
                if len(current_sequence.shape) == 2:
                    # Pour plusieurs features
                    new_point = current_sequence[-1].copy()
                    new_point[0] = next_pred[0, 0]
                    current_sequence = np.vstack([current_sequence[1:], new_point])
                else:
                    # Pour une seule feature
                    current_sequence = np.roll(current_sequence, -1)
                    current_sequence[-1] = next_pred[0, 0]

            # Inverse la normalisation pour obtenir les vrais prix
            # Créer un array avec des zéros pour toutes les features sauf la première (prix)
            inverse_data = np.zeros((len(future_predictions), self.scaler.n_features_in_))
            inverse_data[:, 0] = future_predictions
            
            future_pred_inv = self.scaler.inverse_transform(inverse_data)
            
            return future_pred_inv[:, 0]
            
        except Exception as e:
            logger.error(f"Erreur lors de la prédiction future: {str(e)}")
            logger.error(traceback.format_exc())
            # En cas d'erreur, retourner des prédictions vides
            return np.array([])

    def run_pipeline(self):
        """Exécute le pipeline complet"""
        try:
            logger.info("Chargement des données...")
            df = self.load_data_from_hdfs()
            
            # Vérifie s'il y a suffisamment de données
            if len(df) < self.sequence_length * 2:
                logger.warning(f"Jeu de données trop petit: {len(df)} < {self.sequence_length * 2}")
                if len(df) > 10:
                    # Ajuster la longueur de séquence
                    self.sequence_length = len(df) // 5
                    logger.info(f"Ajustement de sequence_length à {self.sequence_length}")
                else:
                    raise ValueError("Pas assez de données pour l'entraînement")
            
            logger.info("Préparation des données...")
            (X_train, y_train), (X_test, y_test, timestamps_test), features = self.prepare_data(df)
            
            logger.info("Entraînement du modèle...")
            history = self.train_model(X_train, y_train, X_test, y_test)
            
            # Prédictions sur l'ensemble de test
            logger.info("Prédiction sur l'ensemble de test...")
            y_pred = self.model.predict(X_test)
            
            # Inverse la normalisation
            logger.info("Inversion de la normalisation...")
            
            # Préparer les arrays pour l'inversion de la normalisation
            y_test_inv = np.zeros((len(y_test), len(features)))
            y_test_inv[:, 0] = y_test
            
            y_pred_inv = np.zeros((len(y_pred), len(features)))
            y_pred_inv[:, 0] = y_pred[:, 0]
            
            # Inversion
            y_test_inv = self.scaler.inverse_transform(y_test_inv)
            y_pred_inv = self.scaler.inverse_transform(y_pred_inv)
            
            y_test_price = y_test_inv[:, 0]
            y_pred_price = y_pred_inv[:, 0]
            
            # Afficher quelques statistiques
            mse = np.mean((y_test_price - y_pred_price) ** 2)
            rmse = np.sqrt(mse)
            mae = np.mean(np.abs(y_test_price - y_pred_price))
            
            logger.info(f"Métriques du modèle - MSE: {mse:.2f}, RMSE: {rmse:.2f}, MAE: {mae:.2f}")
            
            logger.info("Sauvegarde des prédictions historiques...")
            self.save_predictions_to_hdfs(y_test_price, y_pred_price, timestamps_test)
            
            # Prédictions futures
            logger.info("Génération des prédictions futures...")
            last_sequence = X_test[-1]
            future_predictions = self.predict_future(last_sequence)
            
            if len(future_predictions) > 0:
                logger.info("Sauvegarde des prédictions futures...")
                last_timestamp = timestamps_test[-1]
                self.save_future_predictions(future_predictions, last_timestamp)
            else:
                logger.warning("Pas de prédictions futures générées")
            
            logger.info("Pipeline terminé avec succès")
            
        except Exception as e:
            logger.error(f"Erreur dans le pipeline: {str(e)}")
            logger.error(traceback.format_exc())
            raise

if __name__ == "__main__":
    try:
        # Attendre que le NameNode soit prêt
        logger.info("Attente du démarrage complet du NameNode...")
        time.sleep(30)  # Attente initiale de 30 secondes
        
        # Réduire la longueur de séquence pour commencer
        predictor = BitcoinPredictor(sequence_length=12)
        predictor.run_pipeline()
    except Exception as e:
        logger.error(f"Erreur fatale: {str(e)}")
        logger.error(traceback.format_exc())