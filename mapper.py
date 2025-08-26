#!/usr/bin/env python3

import sys
import json
import re
import os
from datetime import datetime

def mapper_combined():
    # Determine analysis type from environment variable or default to hashtags
    analysis_type = os.environ.get('ANALYSIS_TYPE', 'hashtags').lower()
    
    # Read all input to handle JSON array format
    input_data = []
    for line in sys.stdin:
        input_data.append(line.strip())
    
    # Join all lines and parse as JSON array
    full_input = ''.join(input_data)
    
    try:
        tweets = json.loads(full_input)
        
        # If it's a list of tweets
        if isinstance(tweets, list):
            tweet_list = tweets
        else:
            tweet_list = [tweets]
            
    except json.JSONDecodeError:
        # If JSON parsing fails, try line by line
        tweet_list = []
        for line_data in input_data:
            if line_data and line_data.strip():
                try:
                    tweet = json.loads(line_data)
                    tweet_list.append(tweet)
                except:
                    continue
    
    # Process each tweet
    for tweet in tweet_list:
        try:
            # Get text content - prioritize tweet_text which is the correct field
            text = tweet.get('tweet_text', '') or tweet.get('text', '') or tweet.get('full_text', '') or ''
            if not text:
                continue
            
            # Get creation date and extract month for partitioning
            created_at = tweet.get('timestamp', '') or tweet.get('created_at', '')
            if created_at:
                try:
                    # Handle different date formats
                    if 'T' in created_at:  # ISO format
                        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    else:  # Twitter format
                        dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
                    
                    month_key = dt.strftime('%Y-%m')
                except:
                    month_key = '2024-01'  # default
            else:
                month_key = '2024-01'  # default
            
            # Process based on analysis type
            if analysis_type == 'hashtags':
                # Extract hashtags - first check if already extracted in hashtags field
                hashtag_list = tweet.get('hashtags', [])
                if hashtag_list and isinstance(hashtag_list, list):
                    # Use pre-extracted hashtags
                    for tag in hashtag_list:
                        if isinstance(tag, dict) and 'text' in tag:
                            hashtag_text = tag['text'].lower()
                        else:
                            hashtag_text = str(tag).lower()
                        print("HASHTAG\t{}\t#{}\t1".format(month_key, hashtag_text))
                else:
                    # Extract hashtags from text
                    hashtags = re.findall(r'#\w+', text.lower())
                    for tag in hashtags:
                        print("HASHTAG\t{}\t{}\t1".format(month_key, tag))
            
            elif analysis_type == 'sentiment':
                # Simple sentiment analysis based on positive/negative words
                positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic', 'awesome', 'perfect', 'love', 'best', 'happy', 'nice']
                negative_words = ['bad', 'terrible', 'awful', 'horrible', 'worst', 'hate', 'sad', 'angry', 'disappointed', 'poor', 'fail', 'sucks']
                
                text_lower = text.lower()
                positive_count = sum(1 for word in positive_words if word in text_lower)
                negative_count = sum(1 for word in negative_words if word in text_lower)
                
                if positive_count > negative_count:
                    sentiment = 'positive'
                elif negative_count > positive_count:
                    sentiment = 'negative'
                else:
                    sentiment = 'neutral'
                
                print("SENTIMENT\t{}\t{}\t1".format(month_key, sentiment))
            
            elif analysis_type == 'geography':
                # Extract geographical location if available
                location_info = tweet.get('location', {})
                
                # Extract city information
                city = location_info.get('city', '') if location_info else ''
                if city:
                    city = city.strip()[:50]  # Limit length
                    print("LOCATION\t{}\t{}\t1".format(month_key, city))
                
                # Extract coordinates
                coordinates = location_info.get('coordinates', []) if location_info else []
                if coordinates and len(coordinates) >= 2:
                    # coordinates are [latitude, longitude]
                    lat, lon = coordinates[0], coordinates[1]
                    print("COORDINATES\t{}\t{},{}\t1".format(month_key, lat, lon))
                    
        except Exception as e:
            # Skip problematic tweets
            continue

if __name__ == "__main__":
    mapper_combined()
