#!/usr/bin/env python3
"""
MapReduce pour analyser les tendances de hashtags par mois
"""
import json
import sys
from collections import defaultdict, Counter
from datetime import datetime
import subprocess
import os

class HashtagMapper:
    """
    Mapper pour extraire les hashtags par mois
    """
    
    def map_hashtags(self, tweet):
        """
        Extrait les hashtags d'un tweet avec son mois
        """
        timestamp = tweet.get('timestamp', '')
        hashtags = tweet.get('hashtags', [])
        
        if not timestamp or not hashtags:
            return []
        
        try:
            # Parser le timestamp
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            month_key = f"{dt.year}-{dt.month:02d}"
            
            # Retourner (mois, hashtag) pour chaque hashtag
            results = []
            for hashtag in hashtags:
                # Nettoyer le hashtag
                clean_hashtag = hashtag.strip().lower()
                if clean_hashtag.startswith('#'):
                    clean_hashtag = clean_hashtag[1:]
                
                if clean_hashtag:
                    results.append((month_key, clean_hashtag))
            
            return results
        except Exception as e:
            return []

class HashtagReducer:
    """
    Reducer pour compter les hashtags par mois
    """
    
    def reduce_hashtags(self, mapped_data):
        """
        Compte les hashtags par mois
        """
        # Grouper par mois
        hashtags_by_month = defaultdict(list)
        
        for month, hashtag in mapped_data:
            hashtags_by_month[month].append(hashtag)
        
        # Compter les hashtags pour chaque mois
        monthly_results = {}
        for month, hashtags in hashtags_by_month.items():
            hashtag_counts = Counter(hashtags)
            # Garder les 10 plus populaires
            top_10 = hashtag_counts.most_common(10)
            monthly_results[month] = top_10
        
        return monthly_results

def run_hashtag_mapreduce_local():
    """
    Exécute MapReduce localement sur la structure organisée
    """
    print("=== Analyse MapReduce des Hashtags ===")
    
    base_dir = "tweets_organized"
    if not os.path.exists(base_dir):
        print(f"Dossier {base_dir} introuvable. Exécutez d'abord organize_tweets.py")
        return None
    
    mapper = HashtagMapper()
    all_mapped_data = []
    
    # Phase MAP : traiter tous les fichiers
    print("Phase MAP : Extraction des hashtags...")
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == "tweets.json":
                file_path = os.path.join(root, file)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        tweets = json.load(f)
                    
                    # Mapper chaque tweet
                    for tweet in tweets:
                        mapped_results = mapper.map_hashtags(tweet)
                        all_mapped_data.extend(mapped_results)
                    
                    print(f"Traité: {file_path} ({len(tweets)} tweets)")
                    
                except Exception as e:
                    print(f"Erreur traitement {file_path}: {e}")
    
    print(f"Phase MAP terminée: {len(all_mapped_data)} hashtags extraits")
    
    # Phase REDUCE : compter les hashtags
    print("Phase REDUCE : Comptage des hashtags...")
    
    reducer = HashtagReducer()
    results = reducer.reduce_hashtags(all_mapped_data)
    
    return results

def save_hashtag_results(results, output_file="hashtag_trends.json"):
    """
    Sauvegarde les résultats d'analyse des hashtags
    """
    # Enrichir les résultats avec des métadonnées
    enriched_results = {
        'analysis_type': 'hashtag_trends',
        'generated_at': datetime.now().isoformat(),
        'total_months': len(results),
        'methodology': 'MapReduce - Top 10 hashtags per month',
        'results': results
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_results, f, indent=2, ensure_ascii=False)
    
    print(f"Résultats sauvegardés: {output_file}")

def display_hashtag_trends(results):
    """
    Affiche les tendances de hashtags
    """
    print("\n=== TOP 10 HASHTAGS PAR MOIS ===")
    
    month_names = {
        "01": "Janvier", "02": "Février", "03": "Mars", "04": "Avril",
        "05": "Mai", "06": "Juin", "07": "Juillet", "08": "Août",
        "09": "Septembre", "10": "Octobre", "11": "Novembre", "12": "Décembre"
    }
    
    for month in sorted(results.keys()):
        if month.startswith('2024'):
            month_num = month.split('-')[1]
            month_name = month_names.get(month_num, month)
            
            print(f"\n{month_name} ({month}):")
            
            hashtags = results[month]
            if hashtags:
                for i, (hashtag, count) in enumerate(hashtags, 1):
                    print(f"  {i:2d}. #{hashtag:<20} ({count:3d} mentions)")
            else:
                print("     Aucun hashtag trouvé")

def analyze_hashtag_evolution(results):
    """
    Analyse l'évolution des hashtags dans le temps
    """
    print("\n=== ÉVOLUTION DES HASHTAGS ===")
    
    # Collecter tous les hashtags uniques
    all_hashtags = set()
    for month_data in results.values():
        for hashtag, count in month_data:
            all_hashtags.add(hashtag)
    
    # Analyser les hashtags les plus persistants
    hashtag_months = defaultdict(list)
    for month, month_data in results.items():
        for hashtag, count in month_data:
            hashtag_months[hashtag].append((month, count))
    
    # Hashtags présents dans plusieurs mois
    persistent_hashtags = {
        hashtag: months for hashtag, months in hashtag_months.items() 
        if len(months) >= 3
    }
    
    print(f"Hashtags persistants (présents dans ≥3 mois):")
    for hashtag, months in sorted(persistent_hashtags.items(), 
                                  key=lambda x: len(x[1]), reverse=True):
        total_mentions = sum(count for month, count in months)
        print(f"  #{hashtag}: {len(months)} mois, {total_mentions} mentions totales")

if __name__ == "__main__":
    # Exécuter l'analyse des hashtags
    results = run_hashtag_mapreduce_local()
    
    if results:
        # Afficher les résultats
        display_hashtag_trends(results)
        analyze_hashtag_evolution(results)
        
        # Sauvegarder
        save_hashtag_results(results)
        
        print(f"\nAnalyse des hashtags terminée!")
    else:
        print("Échec de l'analyse des hashtags")