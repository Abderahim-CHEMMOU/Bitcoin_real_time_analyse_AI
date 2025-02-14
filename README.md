# Bitcoin_real_time_analyse_AI

# Traitement en Temps Réel des Données Bitcoin

Ce projet implémente un pipeline de traitement en temps réel des données Bitcoin utilisant Apache Kafka et Apache Spark Streaming. Les données sont collectées depuis les APIs Binance et CoinGecko, puis traitées et stockées dans HDFS.

## Architecture du Système

```
Binance/CoinGecko API → Kafka Producer → Kafka → Spark Streaming → HDFS
```

## Composants

### 1. Kafka Producer (`producer.py`)
- Collecte les données en temps réel depuis :
  - Binance API (prix bid/ask)
  - CoinGecko API (métriques de marché)
- Fréquence de collecte : toutes les 60 secondes
- Données collectées :
  - Prix d'achat (bid)
  - Prix de vente (ask)
  - Volume sur 24h
  - Capitalisation boursière
  - Variations de prix
  - Et plus...

### 2. Spark Streaming (`spark_streaming.py`)
- Consomme les données depuis Kafka
- Effectue des transformations en temps réel :
  - Calcul du spread (différence prix achat/vente)
  - Prix moyen du marché
  - Liquidité totale
  - Volatilité des prix
- Stocke les données dans HDFS :
  - Données brutes enrichies
  - Métriques agrégées par heure

## Structure des Données

### Données Brutes
```python
{
    "timestamp": "2025-02-14T10:58:36.701397",
    "bid_price": 97017.57,
    "ask_price": 97017.58,
    "bid_qty": 0.10025,
    "ask_qty": 7.11458,
    "volume_24h": 30381193367,
    "market_cap": 1923087506901,
    "price_change_24h": 887.8,
    "price_change_percentage_24h": 0.92345,
    "high_24h": 97231,
    "low_24h": 95410,
    "trade_timestamp": 1739530716
}
```

### Métriques Calculées
- Métriques horaires :
  - Prix moyen
  - Spread moyen
  - Liquidité totale
  - Volatilité moyenne
  - Volume moyen
  - Prix haut/bas

## Installation et Configuration

1. **Prérequis**
   - Docker et Docker Compose
   - Python 3.9+
   - Apache Kafka
   - Apache Spark 3.3.0
   - Apache Hadoop 3.3.2

2. **Configuration**
   - Installer les dépendances Python :
     ```bash
     pip install -r requirements.txt
     ```
   - Les fichiers de configuration se trouvent dans :
     - `config/` pour Hadoop
     - `hadoop.env` pour les variables d'environnement

3. **Démarrage**
   ```bash
   docker-compose up -d
   ```

## Structure HDFS

Les données sont stockées dans HDFS selon la structure suivante :
```
/bitcoin/
  ├── raw_data/        # Données brutes enrichies (format Parquet)
  ├── hourly_metrics/  # Métriques agrégées par heure
  └── checkpoints/     # Points de contrôle Spark Streaming
```

## Vérification des Données

Pour vérifier les données stockées dans HDFS :
```bash
# Accéder au conteneur namenode
docker exec -it namenode bash

# Lister les fichiers
hdfs dfs -ls /bitcoin/raw_data
hdfs dfs -ls /bitcoin/hourly_metrics

# Voir le contenu
hdfs dfs -cat /bitcoin/hourly_metrics/*.parquet
```

## Monitoring

Les logs peuvent être consultés avec :
```bash
# Logs du producer Kafka
docker-compose logs -f kafka-producer

# Logs de Spark Streaming
docker-compose logs -f spark-streaming
```

## Métriques et Analyses Disponibles

1. **Métriques de Prix**
   - Prix moyen du marché
   - Spread bid/ask
   - Volatilité des prix

2. **Métriques de Volume**
   - Volume de transactions sur 24h
   - Liquidité disponible
   - Capitalisation boursière

3. **Analyses Temporelles**
   - Agrégations horaires
   - Tendances de prix
   - Variations de volume

# Données

/bitcoin/
├── raw_data/
│   ├── part-00000-xxxxx.parquet  # Données brutes par batch
│   ├── part-00001-xxxxx.parquet
│   └── _SUCCESS                  # Marqueur de succès
├── hourly_metrics/
│   ├── part-00000-xxxxx.parquet  # Métriques agrégées
│   └── _SUCCESS
└── checkpoints/
    ├── offsets/                  # Position dans Kafka
    ├── commits/                  # Commits des transactions
    └── sources/                  # État des sources