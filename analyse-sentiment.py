#!/usr/bin/env python3
"""
MapReduce pour analyser les sentiments des tweets par jour
"""
import json
import sys
import os
import re
from collections import defaultdict
from datetime import datetime

class SimpleSentimentAnalyzer:
    """
    Analyseur de sentiment simple basé sur des mots-clés
    """
    
    def __init__(self):
        # Mots positifs
        self.positive_words = {
            'good', 'great', 'excellent', 'amazing', 'awesome', 'fantastic',
            'wonderful', 'brilliant', 'perfect', 'love', 'best', 'incredible',
            'outstanding', 'remarkable', 'superb', 'marvelous', 'exciting',
            'revolutionizing', 'successful', 'future', 'exciting'
        }
        
        # Mots négatifs
        self.negative_words = {
            'bad', 'terrible', 'awful', 'horrible', 'worst', 'hate', 'disgusting',
            'pathetic', 'useless', 'boring', 'disappointing', 'frustrating',
            'annoying', 'broken', 'failed', 'error', 'problem', 'issue'
        }
        
        # Mots d'intensification
        self.intensifiers = {
            'very', 'extremely', 'really', 'totally', 'completely', 'absolutely',
            'quite', 'rather', 'pretty', 'so', 'too'
        }
    
    def analyze_sentiment(self, text):
        """
        Analyse le sentiment d'un texte
        Retourne un score entre -1 (très négatif) et 1 (très positif)
        """
        if not text:
            return 0.0
        
        # Nettoyer et normaliser le texte
        text = text.lower()
        words = re.findall(r'\b\w+\b', text)
        
        positive_score = 0
        negative_score = 0
        
        for i, word in enumerate(words):
            # Vérifier si le mot précédent est un intensificateur
            intensifier = 1.0
            if i > 0 and words[i-1] in self.intensifiers:
                intensifier = 1.5
            
            if word in self.positive_words:
                positive_score += intensifier
            elif word in self.negative_words:
                negative_score += intensifier
        
        # Calculer le score final
        total_words = len(words)
        if total_words == 0:
            return 0.0
        
        # Normaliser les scores
        positive_normalized = positive_score / total_words
        negative_normalized = negative_score / total_words
        
        # Score final entre -1 et 1
        sentiment_score = positive_normalized - negative_normalized
        
        # Limiter entre -1 et 1
        sentiment_score = max(-1.0, min(1.0, sentiment_score))
        
        return sentiment_score
    
    def get_sentiment_label(self, score):
        """
        Convertit un score en label
        """
        if score > 0.1:
            return "positive"
        elif score < -0.1:
            return "negative"
        else:
            return "neutral"

class SentimentMapper:
    """
    Mapper pour extraire les sentiments par jour
    """
    
    def __init__(self):
        self.analyzer = SimpleSentimentAnalyzer()
    
    def map_sentiment(self, tweet):
        """
        Extrait le sentiment d'un tweet avec sa date
        """
        timestamp = tweet.get('timestamp', '')
        text = tweet.get('tweet_text', '')
        
        if not timestamp or not text:
            return None
        
        try:
            # Parser le timestamp pour extraire la date
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            date_key = f"{dt.year}-{dt.month:02d}-{dt.day:02d}"
            
            # Analyser le sentiment
            sentiment_score = self.analyzer.analyze_sentiment(text)
            sentiment_label = self.analyzer.get_sentiment_label(sentiment_score)
            
            return (date_key, {
                'score': sentiment_score,
                'label': sentiment_label,
                'text_preview': text[:50] + "..." if len(text) > 50 else text
            })
            
        except Exception as e:
            return None

class SentimentReducer:
    """
    Reducer pour calculer les scores moyens de sentiment par jour
    """
    
    def reduce_sentiment(self, mapped_data):
        """
        Calcule les scores moyens de sentiment par jour
        """
        # Grouper par date
        sentiments_by_date = defaultdict(list)
        
        for date, sentiment_data in mapped_data:
            sentiments_by_date[date].append(sentiment_data)
        
        # Calculer les statistiques pour chaque jour
        daily_results = {}
        for date, sentiments in sentiments_by_date.items():
            scores = [s['score'] for s in sentiments]
            labels = [s['label'] for s in sentiments]
            
            # Statistiques
            avg_score = sum(scores) / len(scores) if scores else 0
            positive_count = labels.count('positive')
            negative_count = labels.count('negative')
            neutral_count = labels.count('neutral')
            total_tweets = len(sentiments)
            
            daily_results[date] = {
                'average_sentiment': round(avg_score, 4),
                'total_tweets': total_tweets,
                'sentiment_distribution': {
                    'positive': positive_count,
                    'negative': negative_count,
                    'neutral': neutral_count
                },
                'sentiment_percentages': {
                    'positive': round(positive_count / total_tweets * 100, 2),
                    'negative': round(negative_count / total_tweets * 100, 2),
                    'neutral': round(neutral_count / total_tweets * 100, 2)
                }
            }
        
        return daily_results

def run_sentiment_mapreduce_local():
    """
    Exécute MapReduce localement pour l'analyse de sentiment
    """
    print("=== Analyse MapReduce des Sentiments ===")
    
    base_dir = "tweets_organized"
    if not os.path.exists(base_dir):
        print(f"Dossier {base_dir} introuvable. Exécutez d'abord organize_tweets.py")
        return None
    
    mapper = SentimentMapper()
    all_mapped_data = []
    
    # Phase MAP : analyser tous les tweets
    print("Phase MAP : Analyse des sentiments...")
    
    total_tweets = 0
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == "tweets.json":
                file_path = os.path.join(root, file)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        tweets = json.load(f)
                    
                    # Mapper chaque tweet
                    for tweet in tweets:
                        mapped_result = mapper.map_sentiment(tweet)
                        if mapped_result:
                            all_mapped_data.append(mapped_result)
                            total_tweets += 1
                    
                    print(f"Traité: {file_path} ({len(tweets)} tweets)")
                    
                except Exception as e:
                    print(f"Erreur traitement {file_path}: {e}")
    
    print(f"Phase MAP terminée: {total_tweets} tweets analysés")
    
    # Phase REDUCE : calculer les moyennes par jour
    print("Phase REDUCE : Calcul des moyennes journalières...")
    
    reducer = SentimentReducer()
    results = reducer.reduce_sentiment(all_mapped_data)
    
    return results

def save_sentiment_results(results, output_file="sentiment_analysis.json"):
    """
    Sauvegarde les résultats d'analyse de sentiment
    """
    enriched_results = {
        'analysis_type': 'daily_sentiment_analysis',
        'generated_at': datetime.now().isoformat(),
        'total_days': len(results),
        'methodology': 'MapReduce - Daily average sentiment scores',
        'sentiment_scale': 'Score between -1 (very negative) and 1 (very positive)',
        'results': results
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_results, f, indent=2, ensure_ascii=False)
    
    print(f"Résultats sauvegardés: {output_file}")

def display_sentiment_analysis(results):
    """
    Affiche l'analyse de sentiment
    """
    print("\n=== ANALYSE DE SENTIMENT PAR JOUR ===")
    
    if not results:
        print("Aucun résultat à afficher")
        return
    
    # Trier par date
    sorted_dates = sorted(results.keys())
    
    print(f"Période analysée: {sorted_dates[0]} à {sorted_dates[-1]}")
    print(f"Nombre de jours: {len(results)}")
    
    # Statistiques globales
    all_scores = [data['average_sentiment'] for data in results.values()]
    overall_avg = sum(all_scores) / len(all_scores) if all_scores else 0
    
    print(f"Score moyen global: {overall_avg:.4f}")
    
    # Jours les plus positifs et négatifs
    most_positive = max(results.items(), key=lambda x: x[1]['average_sentiment'])
    most_negative = min(results.items(), key=lambda x: x[1]['average_sentiment'])
    
    print(f"\nJour le plus positif: {most_positive[0]} (score: {most_positive[1]['average_sentiment']:.4f})")
    print(f"Jour le plus négatif: {most_negative[0]} (score: {most_negative[1]['average_sentiment']:.4f})")
    
    # Afficher quelques exemples de jours
    print(f"\nExemples de résultats journaliers:")
    for i, (date, data) in enumerate(sorted(results.items())[:10]):
        sentiment_indicator = "POSITIF" if data['average_sentiment'] > 0.1 else "NÉGATIF" if data['average_sentiment'] < -0.1 else "NEUTRE"
        print(f"  {sentiment_indicator} {date}: {data['average_sentiment']:+.4f} "
              f"({data['total_tweets']} tweets) "
              f"[P:{data['sentiment_percentages']['positive']:.0f}% "
              f"N:{data['sentiment_percentages']['negative']:.0f}% "
              f"Neu:{data['sentiment_percentages']['neutral']:.0f}%]")

def identify_significant_events(results):
    """
    Identifie les événements significatifs basés sur les variations de sentiment
    """
    print("\n=== DÉTECTION D'ÉVÉNEMENTS SIGNIFICATIFS ===")
    
    if len(results) < 3:
        print("Pas assez de données pour détecter les événements")
        return
    
    # Calculer les variations de sentiment
    sorted_dates = sorted(results.keys())
    variations = []
    
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i-1]
        curr_date = sorted_dates[i]
        
        prev_score = results[prev_date]['average_sentiment']
        curr_score = results[curr_date]['average_sentiment']
        
        variation = curr_score - prev_score
        variations.append((curr_date, variation, curr_score))
    
    # Identifier les variations significatives (seuil arbitraire)
    threshold = 0.2
    significant_changes = [
        (date, variation, score) for date, variation, score in variations
        if abs(variation) > threshold
    ]
    
    if significant_changes:
        print(f"Changements significatifs détectés (seuil: ±{threshold}):")
        for date, variation, score in sorted(significant_changes, key=lambda x: abs(x[1]), reverse=True):
            direction = "HAUSSE" if variation > 0 else "BAISSE"
            print(f"  {direction} {date}: {variation:+.4f} (nouveau score: {score:+.4f})")
    else:
        print(f"Aucun changement significatif détecté (seuil: ±{threshold})")

if __name__ == "__main__":
    # Exécuter l'analyse de sentiment
    results = run_sentiment_mapreduce_local()
    
    if results:
        # Afficher les résultats
        display_sentiment_analysis(results)
        identify_significant_events(results)
        
        # Sauvegarder
        save_sentiment_results(results)
        
        print(f"\nAnalyse de sentiment terminée!")
    else:
        print("Échec de l'analyse de sentiment")