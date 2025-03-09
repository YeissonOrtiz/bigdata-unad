# US Election 2020 Tweet Sentiment Analysis

This project analyzes tweets related to the 2020 US Presidential Election to answer three key questions:

1. How did sentiments change before and after Election Day?
2. Were there more negative or positive tweets about the candidates?
3. What keywords were most used in negative tweets?

## Dataset

The analysis uses a dataset of tweets from October 15th to November 8th, 2020, covering the period before and after the US Election Day (November 3rd, 2020). The dataset is sourced from Kaggle: [US Election 2020 Tweets](https://www.kaggle.com/datasets/manchunhui/us-election-2020-tweets).

## Setup and Installation

1. Clone this repository or download the files
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the analysis script:
   ```
   python script.py
   ```

## What the Script Does

1. Downloads the dataset from Kaggle using kagglehub
2. Loads and preprocesses the tweet data
3. Performs sentiment analysis using TextBlob
4. Analyzes sentiment changes before and after Election Day
5. Compares sentiment distribution for tweets mentioning each candidate
6. Identifies key words used in negative tweets
7. Creates visualizations to help understand the findings
8. Outputs a summary of results and saves processed data

## Output

The script creates a `results` directory containing:
- Visualizations of sentiment trends over time
- Comparison of sentiment for each presidential candidate
- Word cloud and bar chart of common words in negative tweets
- A CSV file with the processed data including sentiment scores

## Requirements

- Python 3.6+
- Internet connection to download the dataset
- Libraries listed in requirements.txt

## Notes

- The first run may take some time as it downloads the dataset and NLTK resources
- TextBlob is used for sentiment analysis, which classifies tweets as positive, negative, or neutral
- The script uses a simple approach to identify tweets about candidates based on keyword matching 