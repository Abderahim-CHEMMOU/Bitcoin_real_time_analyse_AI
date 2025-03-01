# Pipeline de Données Bitcoin en Temps Réel

Un système de traitement de données pour collecter, analyser et prédire les prix du Bitcoin en utilisant une architecture Big Data moderne.

## Architecture

Ce projet utilise une architecture Lambda pour traiter les données :
- **Ingestion en temps réel** : Collecte depuis les APIs Binance et CoinGecko
- **Traitement en temps réel** : Transformation et agrégation avec Spark Streaming
- **Stockage distribué** : Persistance des données sur HDFS
- **Analyse prédictive** : Modèle LSTM pour prédire les prix futurs

![Architecture](architecture_diagram.png)

## Composants

Le système comprend les composants suivants :

1. **Kafka Producer** (`producer.py`) :
   - Collecte les données des APIs Binance et CoinGecko
   - Publie les données au format JSON dans le topic Kafka "cryptoTopic"
   - Fonctionne en continu avec un intervalle de rafraîchissement configurable

2. **Spark Streaming** (`spark_streaming.py`) :
   - Consomme les données de Kafka
   - Transforme et enrichit les données (calcul de spread, volatilité, etc.)
   - Stocke les données brutes dans HDFS
   - Agrège les métriques par minute (regroupées par heure)
   - Utilise des checkpoints pour assurer la tolérance aux pannes

3. **Modèle de Prédiction** (`prediction.py`) :
   - Charge les données agrégées depuis HDFS
   - Entraîne un modèle LSTM avec TensorFlow
   - Prédit les prix pour les 24 prochaines heures
   - Sauvegarde les prédictions et les métriques de performance

4. **Infrastructure** :
   - Hadoop (HDFS + YARN) pour le stockage et la gestion des ressources
   - Kafka pour la messagerie
   - Spark pour le traitement distribué
   - Hive pour l'analyse SQL (optionnel)

## Structure des Données

Le pipeline utilise la structure de répertoires HDFS suivante :

```
/bitcoin/
  |- raw_data/           # Données brutes transformées (par minute)
  |- hourly_metrics/     # Métriques agrégées par heure (mais générées par minute)
  |- predictions/        # Prédictions historiques avec métriques de performance
  |- future_predictions/ # Prédictions pour les 24 prochaines heures
  |- prediction_metrics/ # Métriques de performance du modèle
  |- checkpoints/        # Points de contrôle pour la reprise après panne
     |- raw/             # Checkpoints pour le flux de données brutes
     |- metrics/         # Checkpoints pour le flux de métriques agrégées
```

## Détails techniques

### Format des données

Les données collectées incluent :
- Prix d'achat/vente (bid/ask)
- Volumes disponibles
- Volume de trading sur 24h
- Capitalisation du marché
- Variations de prix

### Métriques calculées

Le pipeline calcule en temps réel :
- Spread (écart entre achat et vente)
- Pourcentage de spread
- Prix moyen (mid_price)
- Liquidité totale
- Volatilité des prix

### Agrégation horaire

Actuellement, les métriques sont agrégées par heure mais générées toutes les minutes, ce qui crée plusieurs fichiers de métriques partielles pour chaque heure. Pour une analyse complète, il est recommandé de consolider ces métriques.

### Modèle prédictif

Le modèle LSTM utilise :
- Séquences de 12 points temporels (heures)
- 8 caractéristiques par point
- Structure à double couche LSTM avec dropout
- Métrique d'évaluation : MSE, RMSE, MAE

## Limites connues

1. **Agrégation par micro-batch** : Les métriques horaires sont générées par minute, créant des vues partielles plutôt qu'une agrégation complète par heure.
   
2. **Safe Mode HDFS** : Au démarrage, le NameNode est en mode sécurité, ce qui peut retarder les opérations d'écriture. Le pipeline intègre des mécanismes d'attente et de réessai.

3. **Perte de données Kafka** : En cas de redémarrage des conteneurs, les offsets Kafka peuvent changer. L'option `failOnDataLoss=false` permet au pipeline de continuer malgré ces changements.

## Performances

- Latence de traitement : ~1 minute
- Précision du modèle : RMSE d'environ 300 points sur le prix du Bitcoin
- Capacité : Traitement de plusieurs messages par seconde

## Déploiement

Le système s'exécute dans des conteneurs Docker orchestrés par Docker Compose :

```bash
# Démarrer l'ensemble du pipeline
docker-compose up -d

# Suivre les logs des différents services
docker logs -f kafka-producer
docker logs -f spark-streaming
docker logs -f bitcoin-prediction

# Arrêter le système
docker-compose down
```

## Améliorations futures

1. Implémentation d'une vraie agrégation horaire complète en utilisant les fenêtres temporelles Spark
2. Ajout d'une interface utilisateur pour visualiser les prédictions
3. Amélioration du modèle avec d'autres sources de données (sentiment, actualités)
4. Optimisation de la gestion mémoire pour les traitements Spark
5. Implémentation de tests automatisés et monitoring

## Dépendances

- Python 3.8+
- Apache Spark 3.3.0
- Apache Kafka
- Apache Hadoop 3.2.1
- TensorFlow 2.11.0
- Docker & Docker Compose
