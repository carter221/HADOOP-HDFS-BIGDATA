#!/usr/bin/env python3
"""
MapReduce pour analyser la distribution géographique des tweets
"""
import json
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime

class GeographicMapper:
    """
    Mapper pour extraire les localisations et thèmes des tweets
    """
    
    def __init__(self):
        # Mapping des villes vers les pays
        self.city_to_country = {
            'paris': 'France',
            'london': 'United Kingdom',
            'new york': 'United States',
            'tokyo': 'Japan',
            'berlin': 'Germany',
            'madrid': 'Spain',
            'rome': 'Italy',
            'amsterdam': 'Netherlands',
            'brussels': 'Belgium',
            'vienna': 'Austria',
            'zurich': 'Switzerland',
            'stockholm': 'Sweden',
            'oslo': 'Norway',
            'helsinki': 'Finland',
            'copenhagen': 'Denmark',
            'dublin': 'Ireland',
            'lisbon': 'Portugal',
            'athens': 'Greece',
            'warsaw': 'Poland',
            'prague': 'Czech Republic',
            'budapest': 'Hungary',
            'bucharest': 'Romania',
            'sofia': 'Bulgaria',
            'zagreb': 'Croatia',
            'ljubljana': 'Slovenia',
            'bratislava': 'Slovakia',
            'vilnius': 'Lithuania',
            'riga': 'Latvia',
            'tallinn': 'Estonia'
        }
        
        # Thèmes technologiques
        self.tech_themes = {
            'ai': ['ai', 'artificial intelligence', 'machinelearning', 'ml'],
            'bigdata': ['bigdata', 'hadoop', 'spark', 'analytics'],
            'cloud': ['cloud', 'cloudcomputing', 'aws', 'azure'],
            'blockchain': ['blockchain', 'crypto', 'bitcoin'],
            'iot': ['iot', 'internet of things', 'sensors'],
            'datascience': ['datascience', 'data science', 'analytics'],
            'programming': ['python', 'java', 'javascript', 'coding'],
            'mapreduce': ['mapreduce', 'distributed computing']
        }
    
    def extract_location_info(self, tweet):
        """
        Extrait les informations de localisation d'un tweet
        """
        location = tweet.get('location', {})
        
        if not location or not isinstance(location, dict):
            return None
        
        city = location.get('city', '').lower().strip()
        coordinates = location.get('coordinates', [])
        
        if not city:
            return None
        
        # Mapper la ville vers le pays
        country = self.city_to_country.get(city, 'Unknown')
        
        return {
            'city': city.title(),
            'country': country,
            'coordinates': coordinates
        }
    
    def extract_themes(self, tweet):
        """
        Extrait les thèmes d'un tweet basés sur le texte et les hashtags
        """
        text = tweet.get('tweet_text', '').lower()
        hashtags = [h.lower().replace('#', '') for h in tweet.get('hashtags', [])]
        
        # Combiner texte et hashtags pour l'analyse
        content = text + ' ' + ' '.join(hashtags)
        
        themes = []
        for theme, keywords in self.tech_themes.items():
            if any(keyword in content for keyword in keywords):
                themes.append(theme)
        
        return themes if themes else ['general']
    
    def map_geographic_data(self, tweet):
        """
        Mappe les données géographiques et thématiques d'un tweet
        """
        location_info = self.extract_location_info(tweet)
        
        if not location_info:
            return None
        
        themes = self.extract_themes(tweet)
        timestamp = tweet.get('timestamp', '')
        
        try:
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            date_key = f"{dt.year}-{dt.month:02d}"
        except:
            date_key = "unknown"
        
        return {
            'location': location_info,
            'themes': themes,
            'month': date_key,
            'tweet_text': tweet.get('tweet_text', '')[:100] + "..." if len(tweet.get('tweet_text', '')) > 100 else tweet.get('tweet_text', '')
        }

class GeographicReducer:
    """
    Reducer pour analyser la distribution géographique
    """
    
    def reduce_geographic_data(self, mapped_data):
        """
        Réduit les données géographiques pour l'analyse
        """
        # Structures pour stocker les résultats
        tweets_by_country = defaultdict(list)
        tweets_by_city = defaultdict(list)
        themes_by_country = defaultdict(list)
        themes_by_city = defaultdict(list)
        monthly_geo_data = defaultdict(lambda: defaultdict(int))
        
        # Traiter chaque tweet mappé
        for data in mapped_data:
            country = data['location']['country']
            city = data['location']['city']
            themes = data['themes']
            month = data['month']
            
            # Grouper par pays et ville
            tweets_by_country[country].append(data)
            tweets_by_city[city].append(data)
            
            # Grouper les thèmes
            themes_by_country[country].extend(themes)
            themes_by_city[city].extend(themes)
            
            # Données mensuelles
            monthly_geo_data[month][country] += 1
        
        # Calculer les statistiques
        results = {
            'country_statistics': self._calculate_country_stats(tweets_by_country, themes_by_country),
            'city_statistics': self._calculate_city_stats(tweets_by_city, themes_by_city),
            'monthly_distribution': dict(monthly_geo_data),
            'theme_analysis': self._analyze_themes_by_region(themes_by_country, themes_by_city)
        }
        
        return results
    
    def _calculate_country_stats(self, tweets_by_country, themes_by_country):
        """
        Calcule les statistiques par pays
        """
        country_stats = {}
        
        for country, tweets in tweets_by_country.items():
            themes = themes_by_country[country]
            theme_counts = Counter(themes)
            
            country_stats[country] = {
                'total_tweets': len(tweets),
                'top_themes': theme_counts.most_common(5),
                'cities': list(set([tweet['location']['city'] for tweet in tweets])),
                'sample_tweets': [tweet['tweet_text'] for tweet in tweets[:3]]
            }
        
        return country_stats
    
    def _calculate_city_stats(self, tweets_by_city, themes_by_city):
        """
        Calcule les statistiques par ville
        """
        city_stats = {}
        
        for city, tweets in tweets_by_city.items():
            themes = themes_by_city[city]
            theme_counts = Counter(themes)
            
            # Trouver le pays de cette ville
            country = tweets[0]['location']['country'] if tweets else 'Unknown'
            
            city_stats[city] = {
                'country': country,
                'total_tweets': len(tweets),
                'top_themes': theme_counts.most_common(3),
                'coordinates': tweets[0]['location']['coordinates'] if tweets else []
            }
        
        return city_stats
    
    def _analyze_themes_by_region(self, themes_by_country, themes_by_city):
        """
        Analyse les différences thématiques par région
        """
        # Thèmes globaux
        all_themes = []
        for themes in themes_by_country.values():
            all_themes.extend(themes)
        
        global_theme_counts = Counter(all_themes)
        
        # Analyser les spécificités régionales
        regional_specialties = {}
        
        for country, themes in themes_by_country.items():
            country_theme_counts = Counter(themes)
            total_country_tweets = len(themes)
            
            if total_country_tweets == 0:
                continue
            
            specialties = []
            for theme, count in country_theme_counts.items():
                # Calculer le pourcentage local vs global
                local_percentage = count / total_country_tweets
                global_percentage = global_theme_counts[theme] / len(all_themes)
                
                # Si le thème est sur-représenté localement
                if local_percentage > global_percentage * 1.5 and count >= 2:
                    specialties.append({
                        'theme': theme,
                        'local_percentage': round(local_percentage * 100, 1),
                        'global_percentage': round(global_percentage * 100, 1),
                        'over_representation': round(local_percentage / global_percentage, 2)
                    })
            
            regional_specialties[country] = sorted(specialties, 
                                                 key=lambda x: x['over_representation'], 
                                                 reverse=True)
        
        return {
            'global_themes': global_theme_counts.most_common(10),
            'regional_specialties': regional_specialties
        }

def run_geographic_mapreduce_local():
    """
    Exécute MapReduce localement pour l'analyse géographique
    """
    print("=== Analyse MapReduce Géographique ===")
    
    base_dir = "tweets_organized"
    if not os.path.exists(base_dir):
        print(f"Dossier {base_dir} introuvable. Exécutez d'abord organize_tweets.py")
        return None
    
    mapper = GeographicMapper()
    all_mapped_data = []
    
    # Phase MAP : extraire les données géographiques
    print("Phase MAP : Extraction des données géographiques...")
    
    total_tweets = 0
    tweets_with_location = 0
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == "tweets.json":
                file_path = os.path.join(root, file)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        tweets = json.load(f)
                    
                    # Mapper chaque tweet
                    for tweet in tweets:
                        total_tweets += 1
                        mapped_result = mapper.map_geographic_data(tweet)
                        if mapped_result:
                            all_mapped_data.append(mapped_result)
                            tweets_with_location += 1
                    
                    print(f"Traité: {file_path} ({len(tweets)} tweets)")
                    
                except Exception as e:
                    print(f"Erreur traitement {file_path}: {e}")
    
    print(f"Phase MAP terminée: {tweets_with_location}/{total_tweets} tweets avec localisation")
    
    if not all_mapped_data:
        print("Aucun tweet avec localisation trouvé")
        return None
    
    # Phase REDUCE : analyser la distribution
    print("Phase REDUCE : Analyse de la distribution...")
    
    reducer = GeographicReducer()
    results = reducer.reduce_geographic_data(all_mapped_data)
    
    return results

def save_geographic_results(results, output_file="geographic_analysis.json"):
    """
    Sauvegarde les résultats d'analyse géographique
    """
    enriched_results = {
        'analysis_type': 'geographic_distribution',
        'generated_at': datetime.now().isoformat(),
        'methodology': 'MapReduce - Geographic clustering and theme analysis',
        'total_countries': len(results['country_statistics']),
        'total_cities': len(results['city_statistics']),
        'results': results
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_results, f, indent=2, ensure_ascii=False)
    
    print(f"Résultats sauvegardés: {output_file}")

def display_geographic_analysis(results):
    """
    Affiche l'analyse géographique
    """
    print("\n=== DISTRIBUTION GÉOGRAPHIQUE DES TWEETS ===")
    
    country_stats = results['country_statistics']
    city_stats = results['city_statistics']
    
    # Statistiques par pays
    print(f"\nTweets par pays:")
    sorted_countries = sorted(country_stats.items(), 
                            key=lambda x: x[1]['total_tweets'], 
                            reverse=True)
    
    for country, stats in sorted_countries:
        print(f"  {country}: {stats['total_tweets']} tweets")
        print(f"     Villes: {', '.join(stats['cities'][:3])}")
        if stats['top_themes']:
            top_theme = stats['top_themes'][0]
            print(f"     Thème principal: {top_theme[0]} ({top_theme[1]} mentions)")
    
    # Statistiques par ville
    print(f"\nTop 10 villes:")
    sorted_cities = sorted(city_stats.items(), 
                          key=lambda x: x[1]['total_tweets'], 
                          reverse=True)[:10]
    
    for city, stats in sorted_cities:
        print(f"  {city} ({stats['country']}): {stats['total_tweets']} tweets")
        if stats['top_themes']:
            themes = ', '.join([f"{theme}({count})" for theme, count in stats['top_themes'][:2]])
            print(f"     Thèmes: {themes}")

def display_thematic_differences(results):
    """
    Affiche les différences thématiques par région
    """
    print("\n=== DIFFÉRENCES THÉMATIQUES RÉGIONALES ===")
    
    theme_analysis = results['theme_analysis']
    
    # Thèmes globaux
    print(f"Thèmes globaux les plus populaires:")
    for theme, count in theme_analysis['global_themes'][:5]:
        print(f"  • {theme}: {count} mentions")
    
    # Spécialités régionales
    print(f"\nSpécialités régionales (sur-représentation thématique):")
    
    for country, specialties in theme_analysis['regional_specialties'].items():
        if specialties:
            print(f"\n{country}:")
            for specialty in specialties[:3]:
                print(f"  • {specialty['theme']}: {specialty['local_percentage']}% local "
                      f"vs {specialty['global_percentage']}% global "
                      f"(×{specialty['over_representation']})")

if __name__ == "__main__":
    # Exécuter l'analyse géographique
    results = run_geographic_mapreduce_local()
    
    if results:
        # Afficher les résultats
        display_geographic_analysis(results)
        display_thematic_differences(results)
        
        # Sauvegarder
        save_geographic_results(results)
        
        print(f"\nAnalyse géographique terminée!")
    else:
        print("Échec de l'analyse géographique")