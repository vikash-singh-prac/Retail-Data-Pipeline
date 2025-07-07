
import pandas as pd
import os


def extract_user_purchase(data_dir):

    path = os.path.join(data_dir, 'OnlineRetail.csv')
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path)

    print(f"Extracted {len(df)} user purchases from {path}")
    return df


def extract_movie_review(data_dir):
    path = os.path.join(data_dir, 'movie_review.csv')
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path)

    print(f"Extracted {len(df)} movie reviews from {path}")
    return df
    