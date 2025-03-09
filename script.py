import kagglehub
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from textblob import TextBlob
import re
from wordcloud import WordCloud
from tqdm import tqdm
import nltk
from nltk.corpus import stopwords
from collections import Counter
import os
from datetime import datetime
import warnings
import string  # Add import for string module

# Suppress warnings
warnings.filterwarnings('ignore')

# Set plot style
plt.style.use('fivethirtyeight')
plt.rcParams['figure.figsize'] = (12, 8)

# Download NLTK resources
print("Downloading NLTK resources...")
nltk.download('stopwords')
nltk.download('punkt')
# Make sure nltk is using downloaded resources correctly
try:
    nltk.data.find('tokenizers/punkt')
    print("NLTK punkt tokenizer is available")
except LookupError:
    print("Could not find NLTK punkt tokenizer, using a simpler tokenization approach")

# Download dataset
print("Downloading dataset...")
path = kagglehub.dataset_download("manchunhui/us-election-2020-tweets")
print("Path to dataset files:", path)

# Function to clean tweets
def clean_tweet(tweet):
    # Check if tweet is None or not a string
    if tweet is None or not isinstance(tweet, str):
        return ""
    try:
        # Remove URLs
        tweet = re.sub(r'http\S+|www\S+|https\S+', '', tweet, flags=re.MULTILINE)
        # Remove user mentions
        tweet = re.sub(r'@\w+', '', tweet)
        # Remove hashtags
        tweet = re.sub(r'#\w+', '', tweet)
        # Remove special characters and numbers
        tweet = re.sub(r'[^A-Za-z\s]', '', tweet)
        # Remove extra spaces
        tweet = re.sub(r'\s+', ' ', tweet).strip()
        return tweet
    except Exception as e:
        print(f"Error cleaning tweet: {e}")
        return ""

# Function to get sentiment polarity
def get_sentiment(tweet):
    if not tweet or tweet.strip() == "":
        return 'neutral'
    try:
        analysis = TextBlob(tweet)
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return 'neutral'

# Function to get sentiment polarity score
def get_sentiment_score(tweet):
    if not tweet or tweet.strip() == "":
        return 0
    try:
        analysis = TextBlob(tweet)
        return analysis.sentiment.polarity
    except Exception as e:
        print(f"Error calculating sentiment score: {e}")
        return 0

# Load the dataset
print("Loading the dataset...")
# Find the CSV file in the downloaded path
csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]
if not csv_files:
    raise FileNotFoundError("No CSV file found in the downloaded dataset")

# Use the first CSV file found
dataset_path = os.path.join(path, csv_files[0])
df = pd.read_csv(dataset_path)

print(f"Dataset loaded with {df.shape[0]} rows and {df.shape[1]} columns")
print("Dataset sample:")
print(df.head())

# Convert created_at to datetime with error handling
print("Converting dates and cleaning data...")
df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

# Check for and report NaT values
nat_count = df['created_at'].isna().sum()
if nat_count > 0:
    print(f"Warning: {nat_count} rows have invalid date formats in 'created_at' column")
    print("Removing rows with invalid date formats...")
    # Filter out rows with NaT values
    df = df.dropna(subset=['created_at'])
    print(f"Dataset now has {df.shape[0]} rows after cleaning")

# Add election day reference (November 3, 2020)
election_day = pd.Timestamp('2020-11-03')
df['before_election'] = df['created_at'] < election_day

# Clean the tweets
print("Cleaning tweets...")
df['clean_tweet'] = df['tweet'].apply(clean_tweet)

# Calculate sentiment
print("Performing sentiment analysis...")
tqdm.pandas()
df['sentiment'] = df['clean_tweet'].progress_apply(get_sentiment)
df['sentiment_score'] = df['clean_tweet'].progress_apply(get_sentiment_score)

# Create output directory for results
if not os.path.exists('results'):
    os.makedirs('results')

# 1. How did sentiments change before and after Election Day?
print("\nAnalyzing sentiment changes before and after Election Day...")

# Aggregate data by day and sentiment
df['date'] = df['created_at'].dt.date
sentiment_over_time = df.groupby(['date', 'sentiment']).size().unstack().fillna(0)
sentiment_over_time['total'] = sentiment_over_time.sum(axis=1)
for col in ['positive', 'negative', 'neutral']:
    sentiment_over_time[f'{col}_pct'] = sentiment_over_time[col] / sentiment_over_time['total'] * 100

# Plotting sentiment over time
plt.figure(figsize=(15, 10))
plt.subplot(2, 1, 1)
for sentiment, color in zip(['positive', 'negative', 'neutral'], ['green', 'red', 'blue']):
    plt.plot(sentiment_over_time.index, sentiment_over_time[f'{sentiment}_pct'], 
             marker='o', linestyle='-', linewidth=2, label=sentiment, color=color)

plt.axvline(x=election_day.date(), color='purple', linestyle='--', linewidth=2, label='Election Day')
plt.title('Sentiment Trends Around 2020 US Election', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Percentage of Tweets', fontsize=12)
plt.legend(fontsize=12)
plt.grid(True, alpha=0.3)

# Compute average sentiment score by day
daily_avg_sentiment = df.groupby('date')['sentiment_score'].mean()

plt.subplot(2, 1, 2)
plt.plot(daily_avg_sentiment.index, daily_avg_sentiment.values, 
         marker='o', linestyle='-', linewidth=2, color='blue')
plt.axvline(x=election_day.date(), color='purple', linestyle='--', linewidth=2, label='Election Day')
plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
plt.title('Average Sentiment Score Over Time', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Average Sentiment Score', fontsize=12)
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('results/sentiment_over_time.png')
plt.close()

# 2. Were there more negative or positive tweets about the candidates?
print("\nAnalyzing sentiment distribution for candidate mentions...")

# Define keywords for each candidate
biden_keywords = ['biden', 'joe biden', 'joebiden', 'kamala', 'harris', 'democrat']
trump_keywords = ['trump', 'donald trump', 'donaldtrump', 'pence', 'republican']

# Function to check if tweet mentions a candidate
def contains_candidate(tweet, keywords):
    if not tweet or not isinstance(tweet, str):
        return False
    try:
        tweet = tweet.lower()
        for keyword in keywords:
            if keyword in tweet:
                return True
        return False
    except Exception as e:
        print(f"Error checking candidate mention: {e}")
        return False

# Find tweets mentioning candidates
df['mentions_biden'] = df['clean_tweet'].apply(lambda x: contains_candidate(x, biden_keywords))
df['mentions_trump'] = df['clean_tweet'].apply(lambda x: contains_candidate(x, trump_keywords))

# Calculate sentiment distribution for each candidate
biden_sentiment = df[df['mentions_biden']]['sentiment'].value_counts(normalize=True) * 100
trump_sentiment = df[df['mentions_trump']]['sentiment'].value_counts(normalize=True) * 100

# Plotting candidate sentiment comparison
plt.figure(figsize=(14, 8))
width = 0.35
x = np.arange(3)
sentiment_order = ['positive', 'neutral', 'negative']

biden_values = [biden_sentiment.get(s, 0) for s in sentiment_order]
trump_values = [trump_sentiment.get(s, 0) for s in sentiment_order]

plt.bar(x - width/2, biden_values, width, label='Biden', color='blue', alpha=0.7)
plt.bar(x + width/2, trump_values, width, label='Trump', color='red', alpha=0.7)

plt.xlabel('Sentiment', fontsize=14)
plt.ylabel('Percentage of Tweets', fontsize=14)
plt.title('Sentiment Distribution for Presidential Candidates', fontsize=16)
plt.xticks(x, sentiment_order, fontsize=12)
plt.legend(fontsize=12)
plt.grid(True, alpha=0.3)

for i, v in enumerate(biden_values):
    plt.text(i - width/2, v + 0.5, f'{v:.1f}%', ha='center', va='bottom', fontsize=10, color='blue')
    
for i, v in enumerate(trump_values):
    plt.text(i + width/2, v + 0.5, f'{v:.1f}%', ha='center', va='bottom', fontsize=10, color='red')

plt.tight_layout()
plt.savefig('results/candidate_sentiment_comparison.png')
plt.close()

# Summary statistics
biden_mentions = df['mentions_biden'].sum()
trump_mentions = df['mentions_trump'].sum()

print(f"Biden was mentioned in {biden_mentions} tweets ({biden_mentions/len(df)*100:.1f}% of total)")
print(f"Trump was mentioned in {trump_mentions} tweets ({trump_mentions/len(df)*100:.1f}% of total)")
print("\nSentiment distribution for Biden mentions:")
print(biden_sentiment)
print("\nSentiment distribution for Trump mentions:")
print(trump_sentiment)

# 3. What keywords were most used in negative tweets?
print("\nAnalyzing keywords in negative tweets...")

try:
    # Get stopwords
    stop_words = set(stopwords.words('english'))
    additional_stopwords = {'amp', 'rt', 'u', 'get', 'lol', 'go', 'got', 'make', 'us', 'one', 'will'}
    stop_words.update(additional_stopwords)
    
    # Define a simple tokenizer as fallback
    def simple_tokenize(text):
        """Simple tokenization function that splits on whitespace and removes punctuation"""
        if not text or not isinstance(text, str):
            return []
        # Convert to lowercase and remove punctuation
        text = text.lower()
        for p in string.punctuation:
            text = text.replace(p, ' ')
        # Split on whitespace and filter out empty strings
        return [token for token in text.split() if token.strip()]
    
    # Function to extract keywords from tweets
    def extract_keywords(tweets):
        words = []
        for tweet in tweets:
            if not tweet or not isinstance(tweet, str):
                continue
            try:
                # First try NLTK tokenization
                try:
                    word_tokens = nltk.word_tokenize(tweet.lower())
                except Exception as e:
                    # Fall back to simple tokenization if NLTK fails
                    print(f"NLTK tokenization failed, using simple tokenization: {e}")
                    word_tokens = simple_tokenize(tweet)
                
                filtered_words = [word for word in word_tokens if word.isalpha() and word not in stop_words and len(word) > 2]
                words.extend(filtered_words)
            except Exception as e:
                print(f"Error extracting keywords: {e}")
        return words
    
    # Extract keywords from negative tweets
    negative_tweets = df[df['sentiment'] == 'negative']['clean_tweet'].tolist()
    print(f"Analyzing {len(negative_tweets)} negative tweets for keywords...")
    negative_keywords = extract_keywords(negative_tweets)
    print(f"Extracted {len(negative_keywords)} keywords from negative tweets")
    keyword_counts = Counter(negative_keywords).most_common(100)

    # Check if we have any keywords before creating wordcloud
    if not keyword_counts:
        print("Warning: No keywords extracted from negative tweets. Cannot create word cloud.")
        # Create a dummy word count to avoid the error
        keyword_counts = [("no_negative_keywords_found", 1)]

    # Create WordCloud for negative tweets
    wordcloud = WordCloud(width=800, height=400, background_color='white', 
                          max_words=100, colormap='Reds', contour_width=1, contour_color='grey')
    wordcloud.generate_from_frequencies(dict(keyword_counts))
    
    plt.figure(figsize=(12, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Most Common Words in Negative Tweets', fontsize=16)
    plt.tight_layout()
    plt.savefig('results/negative_keywords_wordcloud.png')
    plt.close()
    
    # Creating a bar plot for the top keywords in negative tweets
    if keyword_counts and keyword_counts[0][0] != "no_negative_keywords_found":
        # Determine how many keywords to display
        top_n = min(20, len(keyword_counts))
        top_keywords = keyword_counts[:top_n]
        words, counts = zip(*top_keywords)
        
        plt.figure(figsize=(14, 8))
        plt.barh(range(len(words)), counts, color='red', alpha=0.7)
        plt.yticks(range(len(words)), words)
        plt.xlabel('Frequency', fontsize=12)
        plt.title(f'Top {top_n} Keywords in Negative Tweets', fontsize=16)
        plt.gca().invert_yaxis()  # To have the most frequent at the top
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('results/negative_keywords_barchart.png')
        plt.close()
        print(f"Created bar chart with top {top_n} keywords in negative tweets")
    else:
        print("Skipping bar chart creation due to insufficient keyword data")

    # Save processed data
    try:
        print("Saving processed data to CSV...")
        df.to_csv('results/processed_tweets.csv', index=False)
        print("Data saved successfully")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
    
    print("\nAnalysis complete! Results saved in the 'results' directory.")
    print("Summary of findings:")
    print(f"1. Analyzed {len(df)} tweets between {df['date'].min()} and {df['date'].max()}")
    print(f"2. Overall sentiment distribution: {df['sentiment'].value_counts(normalize=True) * 100}")
    print(f"3. Average sentiment before election: {df[df['before_election']]['sentiment_score'].mean():.3f}")
    print(f"4. Average sentiment after election: {df[~df['before_election']]['sentiment_score'].mean():.3f}")
    print("5. Check the generated visualizations in the 'results' folder for detailed insights.")

except Exception as e:
    print(f"\nAn error occurred during analysis: {e}")
    import traceback
    traceback.print_exc()
    print("\nThe script encountered an error. Please check the error message above for more information.")