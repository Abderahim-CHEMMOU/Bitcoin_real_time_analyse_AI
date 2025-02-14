from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime

class BitcoinDataProducer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = 'cryptoTopic'

    def get_bitcoin_data(self):
        try:
            # Récupération des données via CoinGecko
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price',
                params={
                    'ids': 'bitcoin',
                    'vs_currencies': 'usd',
                    'include_24hr_vol': True,
                    'include_last_updated_at': True
                }
            )
            data = response.json()['bitcoin']
            
            # Création du message
            message = {
                'timestamp': datetime.now().isoformat(),
                'price_usd': data['usd'],
                'volume_24h': data.get('usd_24h_vol', 0),
                'last_updated': data['last_updated_at']
            }
            return message
        except Exception as e:
            print(f"Erreur lors de la récupération des données: {e}")
            return None

    def start_producing(self, interval=60):
        while True:
            data = self.get_bitcoin_data()
            if data:
                try:
                    self.producer.send(self.topic, value=data)
                    print(f"Données envoyées: {data}")
                except Exception as e:
                    print(f"Erreur lors de l'envoi des données: {e}")
            time.sleep(interval)  # Attendre 60 secondes avant la prochaine requête

if __name__ == "__main__":
    producer = BitcoinDataProducer(bootstrap_servers=['kafka:9092'])
    producer.start_producing()