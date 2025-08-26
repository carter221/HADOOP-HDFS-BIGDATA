import json
import os
from datetime import datetime
from collections import defaultdict
import shutil

def load_tweets_from_json(json_file):
    """
    Charge les tweets depuis le fichier JSON
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            tweets = json.load(f)
        print(f"{len(tweets)} tweets chargés depuis {json_file}")
        return tweets
    except Exception as e:
        print(f"Erreur lors du chargement du fichier JSON: {e}")
        return []

def analyze_data_structure(tweets):
    """
    Analyse la structure des données
    """
    if not tweets:
        return
    
    print("\n=== Analyse de la structure des données ===")
    sample_tweet = tweets[0]
    print("Exemple de tweet:")
    for key, value in sample_tweet.items():
        print(f"  {key}: {type(value).__name__} = {value}")
    
    # Analyser les timestamps
    print("\n=== Analyse des timestamps ===")
    timestamp_formats = set()
    years = set()
    
    for tweet in tweets[:10]:  # Analyser les 10 premiers
        timestamp = tweet.get('timestamp', '')
        if timestamp:
            timestamp_formats.add(type(timestamp).__name__)
            try:
                if 'T' in str(timestamp):
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                else:
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                years.add(dt.year)
            except Exception as e:
                print(f"Format timestamp non reconnu: {timestamp}")
    
    print(f"Formats de timestamp trouvés: {timestamp_formats}")
    print(f"Années trouvées: {sorted(years)}")

def organize_tweets_by_month(tweets):
    """
    Organise les tweets par année/mois
    """
    tweets_by_month = defaultdict(list)
    errors = []
    
    print("\n=== Organisation par mois ===")
    
    for i, tweet in enumerate(tweets):
        timestamp = tweet.get('timestamp', '')
        
        if not timestamp:
            errors.append(f"Tweet {i}: pas de timestamp")
            tweets_by_month["unknown"].append(tweet)
            continue
        
        try:
            # Parser le timestamp (format: "2024-05-25 21:37:21")
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            # Vérifier que c'est 2024
            if dt.year == 2024:
                month_key = f"{dt.year}-{dt.month:02d}"
                tweets_by_month[month_key].append(tweet)
            else:
                errors.append(f"Tweet {i}: année {dt.year} (pas 2024)")
                tweets_by_month[f"year_{dt.year}"].append(tweet)
                
        except Exception as e:
            errors.append(f"Tweet {i}: erreur parsing '{timestamp}' - {e}")
            tweets_by_month["unknown"].append(tweet)
    
    if errors:
        print(f"{len(errors)} erreurs trouvées:")
        for error in errors[:5]:  # Afficher seulement les 5 premières
            print(f"  {error}")
        if len(errors) > 5:
            print(f"  ... et {len(errors) - 5} autres erreurs")
    
    return tweets_by_month

def create_local_structure_and_files(tweets_by_month, base_dir="tweets_organized"):
    """
    Crée la structure de dossiers locale et sauvegarde les fichiers JSON
    """
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    
    os.makedirs(base_dir, exist_ok=True)
    
    print(f"\n=== Création de la structure locale ===")
    
    total_tweets = 0
    monthly_stats = []
    
    for month_key, tweets_list in sorted(tweets_by_month.items()):
        if tweets_list:
            if month_key == "unknown":
                dir_path = os.path.join(base_dir, "year=2024", "month=unknown")
                month_name = "Date inconnue"
            elif month_key.startswith("year_"):
                year = month_key.split('_')[1]
                dir_path = os.path.join(base_dir, f"year={year}", "month=mixed")
                month_name = f"Année {year} (mixte)"
            else:
                year, month = month_key.split('-')
                dir_path = os.path.join(base_dir, f"year={year}", f"month={month}")
                month_name = get_month_name(month)
            
            # Créer le répertoire
            os.makedirs(dir_path, exist_ok=True)
            
            # Enrichir les tweets avec des métadonnées
            enriched_tweets = []
            for tweet in tweets_list:
                enriched_tweet = tweet.copy()
                enriched_tweet['_metadata'] = {
                    'processed_at': datetime.now().isoformat(),
                    'partition_key': month_key,
                    'total_hashtags': len(tweet.get('hashtags', [])),
                    'has_location': 'location' in tweet and tweet['location'] is not None
                }
                enriched_tweets.append(enriched_tweet)
            
            # Sauvegarder le fichier JSON
            file_path = os.path.join(dir_path, "tweets.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(enriched_tweets, f, indent=2, ensure_ascii=False)
            
            total_tweets += len(tweets_list)
            monthly_stats.append({
                'month': month_name,
                'key': month_key,
                'count': len(tweets_list),
                'path': file_path
            })
            
            print(f"{month_name}: {len(tweets_list)} tweets -> {file_path}")
    
    # Créer un fichier de statistiques
    stats_file = os.path.join(base_dir, "statistics.json")
    stats = {
        'total_tweets': total_tweets,
        'total_partitions': len(monthly_stats),
        'partitions': monthly_stats,
        'generated_at': datetime.now().isoformat()
    }
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"\nStatistiques sauvegardées: {stats_file}")
    print(f"Total: {total_tweets} tweets organisés en {len(monthly_stats)} partitions")
    print(f"Structure créée dans: {base_dir}/")
    
    return base_dir

def get_month_name(month_num):
    """
    Retourne le nom du mois en français
    """
    months = {
        "01": "Janvier", "02": "Février", "03": "Mars", "04": "Avril",
        "05": "Mai", "06": "Juin", "07": "Juillet", "08": "Août",
        "09": "Septembre", "10": "Octobre", "11": "Novembre", "12": "Décembre"
    }
    return months.get(month_num, f"Mois {month_num}")

def generate_summary_report(base_dir):
    """
    Génère un rapport de synthèse
    """
    print("\n=== Rapport de synthèse ===")
    
    # Lire les statistiques
    stats_file = os.path.join(base_dir, "statistics.json")
    if os.path.exists(stats_file):
        with open(stats_file, 'r', encoding='utf-8') as f:
            stats = json.load(f)
        
        print(f"Total tweets: {stats['total_tweets']}")
        print(f"Partitions créées: {stats['total_partitions']}")
        print(f"Généré le: {stats['generated_at']}")
        
        print("\nRépartition par mois:")
        for partition in stats['partitions']:
            print(f"  • {partition['month']}: {partition['count']} tweets")
        
        # Analyser quelques métriques
        total_by_year = defaultdict(int)
        for partition in stats['partitions']:
            if 'year=' in partition['path']:
                year = partition['path'].split('year=')[1].split('/')[0]
                total_by_year[year] += partition['count']
        
        print(f"\nRépartition par année:")
        for year, count in sorted(total_by_year.items()):
            print(f"  • {year}: {count} tweets")

if __name__ == "__main__":
    print("=== Organisation des tweets réels 2024 ===")
    
    # Chemin vers votre fichier JSON
    json_file = "tweets_with_locations.json"
    
    if not os.path.exists(json_file):
        print(f"Fichier JSON non trouvé: {json_file}")
        print("Veuillez vous assurer que le fichier existe dans le répertoire courant.")
        exit(1)
    
    # Charger les tweets
    tweets = load_tweets_from_json(json_file)
    
    if not tweets:
        print("Aucun tweet chargé. Arrêt du programme.")
        exit(1)
    
    # Analyser la structure
    analyze_data_structure(tweets)
    
    # Organiser par mois
    tweets_by_month = organize_tweets_by_month(tweets)
    
    # Créer la structure locale
    local_dir = create_local_structure_and_files(tweets_by_month)
    
    # Générer le rapport
    generate_summary_report(local_dir)
    
    print(f"\nOrganisation locale terminée!")
    print(f"Dossier créé: {local_dir}")
    print(f"\nPour pousser vers HDFS, utilisez:")
    print(f"python push_to_hdfs.py")