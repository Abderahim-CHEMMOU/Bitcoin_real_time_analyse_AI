from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime
import logging
from kafka.errors import NoBrokersAvailable
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitcoinDataProducer:
    def __init__(self, bootstrap_servers, max_retries=5, retry_delay=5):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.topic = 'cryptoTopic'
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connect()

    def connect(self):
        retries = 0
        while retries < self.max_retries:
            try:
                logger.info(f"Tentative de connexion à Kafka (essai {retries + 1}/{self.max_retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    api_version=(0, 10, 1)
                )
                logger.info("Connecté à Kafka avec succès!")
                return
            except NoBrokersAvailable:
                retries += 1
                if retries < self.max_retries:
                    logger.warning(f"Impossible de se connecter à Kafka. Nouvelle tentative dans {self.retry_delay} secondes...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Impossible de se connecter à Kafka après plusieurs tentatives.")
                    raise

    def get_bitcoin_data(self):
        try:
            # Récupération du carnet d'ordres de Binance
            response_orderbook = requests.get('https://api.binance.com/api/v3/ticker/bookTicker', params={'symbol': 'BTCUSDT'})
            orderbook_data = response_orderbook.json()
            
            # Récupération des données de marché de CoinGecko
            response_market = requests.get(
                'https://api.coingecko.com/api/v3/coins/markets',
                params={
                    'vs_currency': 'usd',
                    'ids': 'bitcoin',
                    'order': 'market_cap_desc',
                    'per_page': 1,
                    'page': 1,
                    'sparkline': False
                }
            )
            market_data = response_market.json()[0]
            
            current_time = datetime.now().isoformat()
            
            message = {
                'timestamp': current_time,
                'bid_price': float(orderbook_data['bidPrice']),  # Prix d'achat (meilleure offre d'achat)
                'ask_price': float(orderbook_data['askPrice']),  # Prix de vente (meilleure offre de vente)
                'bid_qty': float(orderbook_data['bidQty']),      # Quantité disponible à l'achat
                'ask_qty': float(orderbook_data['askQty']),      # Quantité disponible à la vente
                'volume_24h': market_data['total_volume'],
                'market_cap': market_data['market_cap'],
                'price_change_24h': market_data['price_change_24h'],
                'price_change_percentage_24h': market_data['price_change_percentage_24h'],
                'high_24h': market_data['high_24h'],
                'low_24h': market_data['low_24h'],
                'trade_timestamp': int(time.time())
            }
            
            logger.info(f"Données récupérées avec succès: {message}")
            return message
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données: {str(e)}")
            return None

    def start_producing(self, interval=60):
        while True:
            try:
                data = self.get_bitcoin_data()
                if data and self.producer:
                    self.producer.send(self.topic, value=data)
                    logger.info(f"Données envoyées: {data}")
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Erreur lors de l'envoi des données: {e}")
                try:
                    self.connect()  # Tentative de reconnexion
                except Exception as conn_error:
                    logger.error(f"Échec de la reconnexion: {conn_error}")
                    time.sleep(interval)

if __name__ == "__main__":
    try:
        producer = BitcoinDataProducer(bootstrap_servers=['kafka:9092'])
        producer.start_producing()
    except KeyboardInterrupt:
        logger.info("Arrêt du producer...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        sys.exit(1)