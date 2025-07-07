
from scripts.sentiment_analysis import analyze_sentiment

input_path = 'data/movie_review.csv'
output_path = 'data/movie_review_sentiment.csv'


analyze_sentiment(input_path, output_path)

print(f"Sentiment analysis completed. Results saved to {output_path}")