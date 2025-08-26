# Data Lakehouse - Analyse de Tweets avec Hadoop MapReduce

TP noté  - Architecture Data Lakehouse pour l'analyse de 22,000+ tweets avec Hadoop et MapReduce.

## 🚀 Démarrage

```bash
# 1. Lancer l'environnement
docker-compose up -d

# 2. Organiser les données
python json-organize.py

# 3. Upload vers HDFS
python local-to-hdfs.py

# 4. Lancer les analyses
./run-hadoop-streaming.sh sentiment
./run-hadoop-streaming.sh geography
./run-hadoop-streaming.sh hashtags
```

## 📊 Analyses disponibles

### Analyse de sentiment
```bash
./run-hadoop-streaming.sh sentiment
```
Classification : positif/négatif/neutre

### Distribution géographique  
```bash
./run-hadoop-streaming.sh geography
```
Mapping par villes avec coordonnées GPS

### Hashtags populaires
```bash
./run-hadoop-streaming.sh hashtags
```

## 🔍 Résultats

Fichiers générés dans HDFS (format .txt) :
```
/user/data/output/mapreduce_results/
├── sentiment/results_sentiment.txt
├── geography/results_geography.txt  
└── hashtags/results_hashtags.txt
```

## 🔧 Interfaces web

- **HDFS** : http://localhost:9870
- **YARN** : http://localhost:8088
- **History** : http://localhost:8188

## 🛠️ Prérequis

- Docker & Docker Compose
- Python 3.8+
- 8GB RAM minimum

### Commandes de vérification HDFS
```bash
# Vérifier la structure des résultats Hadoop Streaming
docker exec namenode hdfs dfs -ls /user/data/output/mapreduce_results/

# Lister les fichiers .txt générés
docker exec namenode hdfs dfs -ls /user/data/output/mapreduce_results/sentiment/
docker exec namenode hdfs dfs -ls /user/data/output/mapreduce_results/geography/  
docker exec namenode hdfs dfs -ls /user/data/output/mapreduce_results/hashtags/

# Consulter les résultats d'analyse
docker exec namenode hdfs dfs -cat /user/data/output/mapreduce_results/sentiment/results_sentiment.txt
docker exec namenode hdfs dfs -cat /user/data/output/mapreduce_results/geography/results_geography.txt  
docker exec namenode hdfs dfs -cat /user/data/output/mapreduce_results/hashtags/results_hashtags.txt

# Statistiques des fichiers de résultats
docker exec namenode hdfs dfs -du -h /user/data/output/mapreduce_results/
```


