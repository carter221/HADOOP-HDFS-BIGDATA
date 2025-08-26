#!/usr/bin/env python3

import sys
import os
from collections import Counter

def reducer_combined():
    # Determine analysis type from environment variable or default to hashtags
    analysis_type = os.environ.get('ANALYSIS_TYPE', 'hashtags').lower()
    
    # Counter for aggregating data
    data_counter = Counter()
    
    # Process input from mappers
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            # Parse mapper output: TYPE\tMONTH\tITEM\tCOUNT
            parts = line.split('\t')
            if len(parts) >= 4:
                data_type = parts[0]
                month = parts[1]
                item = parts[2]
                count = int(parts[3])
                
                # Group by the item (hashtag, sentiment, location, etc.)
                key = "{}\t{}".format(month, item)
                data_counter[key] += count
        
        except (ValueError, IndexError):
            # Skip malformed lines
            continue
    
    # Output results based on analysis type
    if analysis_type == 'hashtags':
        # Sort by count descending and output top 10
        sorted_items = data_counter.most_common(10)
        for key, count in sorted_items:
            month, hashtag = key.split('\t', 1)
            print("HASHTAG_RESULT\t{}\t{}\t{}".format(month, hashtag, count))
    
    elif analysis_type == 'sentiment':
        # Sort by count descending
        sorted_items = data_counter.most_common()
        for key, count in sorted_items:
            month, sentiment = key.split('\t', 1)
            print("SENTIMENT_RESULT\t{}\t{}\t{}".format(month, sentiment, count))
    
    elif analysis_type == 'geography':
        # Sort by count descending and output top 20 locations
        sorted_items = data_counter.most_common(20)
        for key, count in sorted_items:
            month, location = key.split('\t', 1)
            print("GEOGRAPHY_RESULT\t{}\t{}\t{}".format(month, location, count))

if __name__ == "__main__":
    reducer_combined()
