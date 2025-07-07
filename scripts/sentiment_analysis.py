import pandas as pd
from textblob import TextBlob


def analyze_sentiment(input_path, output_path):

    df = pd.read_csv(input_path)

    def get_sentiment(text):
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity
        return 'positive' if polarity > 0 else 'negative' if polarity < 0 else 'neutral'

    df['sentiment'] = df['review_str'].apply(get_sentiment)
    df[['cid', 'sentiment']].to_csv(output_path, index=False)

