# Projet d'Analyse Big Data pour le Transport

## Vue d'ensemble
Ce projet met en œuvre une solution d'analyse big data en temps réel pour le transport public en Finlande. Il intègre plusieurs sources de données telles que les positions des véhicules en direct, les mises à jour en temps réel GTFS et les données météorologiques pour fournir des analyses en temps réel, des prédictions et des analyses basées sur des graphes.

## Architecture et Flux de Données
### Architecture Générale
Le projet repose sur une architecture distribuée basée sur les technologies **Apache Kafka, Apache Spark, PostgreSQL (Neon) et Neo4j**. 

1. **Sources de données**
   - Les positions des véhicules sont récupérées en temps réel via MQTT depuis l’API de Helsinki Regional Transport (HSL).
   - Les mises à jour GTFS-RT des trajets sont collectées via l’API HSL.
   - Les données météorologiques sont récupérées à partir de l’API OpenWeatherMap.

2. **Ingestion des Données**
   - Les données sont collectées et envoyées dans **Kafka** via des producteurs Python.
   - Les messages Kafka sont stockés dans différents **topics** (vehicle-data, trip-updates, weather-data).

3. **Traitement en Temps Réel**
   - **Spark Streaming** récupère les flux de Kafka et applique des transformations pour nettoyer et enrichir les données.
   - Les données nettoyées sont ensuite envoyées vers **PostgreSQL (Neon)** pour stockage et analyse historique.
   - Certaines données spécifiques sont formatées et envoyées vers **Neo4j** pour des analyses basées sur des graphes.

4. **Machine Learning & Analytique**
   - **Spark MLlib** est utilisé pour entraîner un modèle de prédiction des retards.
   - **GraphX/GraphFrames** permettent d’analyser les trajets et détecter des incohérences ou des problèmes de synchronisation des trajets.

5. **Visualisation et Monitoring**
   - Les résultats analytiques sont affichés via **Grafana** pour le suivi en temps réel.
   - Des visualisations statiques et dynamiques sont créées avec **Matplotlib et Pandas**.

### Flux de Données
1. **Les données des véhicules et des trajets sont récupérées par les producteurs Kafka.**
2. **Kafka stocke ces données dans des topics spécifiques.**
3. **Spark Streaming consomme ces flux de données et effectue des transformations.**
4. **Les données traitées sont stockées dans PostgreSQL (Neon) et Neo4j selon leur utilisation.**
5. **Les modèles ML sont entraînés et utilisés pour la prédiction des retards.**
6. **Les résultats sont visualisés et accessibles via des dashboards en temps réel.**

## Structure du Projet
```
BigData_Transport_Project/
│── data/
│   ├── raw/              # Données brutes collectées
│   ├── processed/        # Données traitées et nettoyées
│   ├── neo4j_import/     # Données formatées pour l'importation Neo4j
│── src/
│   ├── ingestion/        # Scripts d'ingestion des données
│   │   ├── ingest_vehicle_positions_kafka.py
│   │   ├── ingest_gtfs_trip_updates_kafka.py
│   │   ├── ingest_weather_kafka.py
│   ├── streaming/        # Traitement en temps réel
│   │   ├── spark_read_kafka.py
│   │   ├── spark_join_kafka_streams.py
│   │   ├── stream_vehicle_processing.py
│   ├── analytics/        # Analytique et modèles ML
│   │   ├── train_spark_mllib.py
│   │   ├── transport_delay_prediction_pipeline.py
│   ├── models/           # Modèles de machine learning
│   ├── graph/            # Analyse des graphes avec Neo4j
│   ├── visualization/    # Scripts de visualisation des données
│── output/               # Rapports, données traitées, résultats des modèles
│── requirements.txt      # Dépendances
│── docker-compose.yml    # Configuration Docker pour Spark, Kafka, Neo4j et PostgreSQL (Neon)
│── README.md             # Documentation
```

## Fonctionnalités
### Ingestion des Données
- **Positions des Véhicules :** Récupérées via MQTT de Helsinki Regional Transport (HSL) et envoyées vers Kafka.
- **Mises à Jour GTFS :** Récupérées de l'API GTFS-RT de HSL et publiées dans Kafka.
- **Données Météorologiques :** Collectées à partir de l'API OpenWeatherMap et envoyées vers Kafka.

### Traitement en Temps Réel
- **Streaming Kafka :** Spark Streaming lit les données depuis les topics Kafka.
- **Nettoyage et Transformation des Données :** Positions des véhicules, mises à jour GTFS et données météorologiques sont traitées pour l'analyse.
- **Intégration avec PostgreSQL (Neon) :** Stockage des données dans une base de données PostgreSQL gérée sur Neon.
- **Stockage des données pour Neo4j :** Formatage des données pour leur importation dans une base de graphes Neo4j.

### Machine Learning & Analytique
- **Prédiction des Retards :** Entraînement d'un modèle avec Spark MLlib pour prévoir les retards de transport.
- **Analyse des Graphes avec Neo4j :** Utilisation de GraphX/GraphFrames et stockage des données dans Neo4j pour détecter les problèmes de synchronisation des trajets.

### Visualisation
- **Grafana & Python (Matplotlib & Pandas) :** Utilisés pour créer des tableaux de bord en temps réel et des rapports.

## Instructions d'Installation
### Prérequis
Assurez-vous d'avoir installé :
- Docker & Docker Compose
- Apache Spark
- Apache Kafka
- Neo4j (en ligne ou en local)
- PostgreSQL (Neon)
- Python 3.x avec les bibliothèques requises (`pip install -r requirements.txt`)

### Exécution du Projet
1. **Démarrer les Conteneurs Kafka, Spark et Neo4j :**
   ```sh
   docker-compose up -d
   ```
2. **Lancer les Scripts d'Ingestion des Données :**
   ```sh
   python src/ingestion/ingest_vehicle_positions_kafka.py
   python src/ingestion/ingest_gtfs_trip_updates_kafka.py
   python src/ingestion/ingest_weather_kafka.py
   ```
3. **Démarrer le Streaming Spark :**
   ```sh
   python src/streaming/spark_read_kafka.py
   python src/streaming/spark_join_kafka_streams.py
   ```
4. **Entraîner le Modèle ML :**
   ```sh
   python src/models/train_spark_mllib.py
   ```
5. **Exécuter le Pipeline de Prédiction des Retards :**
   ```sh
   python src/models/transport_delay_prediction_pipeline.py
   ```
6. **Exporter les Données vers PostgreSQL :**
   ```sh
   python src/models/DBconception.py
   ```
7. ** Creation du Graph et stockage dans Neo4j: **
   ```sh
   python src/graph/transport_graph.py
   ```   ```sh
   python src/graph/grapthdemo.py
   ```
