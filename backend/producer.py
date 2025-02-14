from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime

class BitcoinDataProducer:
    def __init__(self, bootstrap_servers, api_key, topic='api_data_coingecko'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
        self.api_key = api_key

    def get_bitcoin_data(self):
        try:
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price',
                params={
                    'ids': 'bitcoin',
                    'vs_currencies': 'usd',
                    'include_market_cap': 'true',
                    'include_24hr_vol': 'true',
                    'include_24hr_change': 'true',
                    'include_last_updated_at': 'true',
                    'x_cg_api_key': self.api_key
                }
            )
            response.raise_for_status()
            data = response.json()['bitcoin']
            message = {
                'timestamp': datetime.now().isoformat(),
                'price_usd': data['usd'],
                'market_cap_usd': data.get('usd_market_cap', 0),
                'volume_24h_usd': data.get('usd_24h_vol', 0),
                'change_24h_usd': data.get('usd_24h_change', 0),
                'last_updated': data['last_updated_at']
            }
            return message
        except Exception as e:
            print(f"Erreur lors de la récupération des données: {e}")
            return None

    def start_producing(self, interval=90, max_requests=20):
        for i in range(max_requests):
            data = self.get_bitcoin_data()
            if data:
                try:
                    self.producer.send(self.topic, value=data)
                    print(f"Données envoyées: {data}")
                except Exception as e:
                    print(f"Erreur lors de l'envoi des données: {e}")
            time.sleep(interval)

if __name__ == "__main__":
    API_KEY = 'CG-NC8kDN1R1Ej428kKe3qnEquD'
    producer = BitcoinDataProducer(bootstrap_servers=['kafka:9092'], api_key=API_KEY)
    producer.start_producing()
