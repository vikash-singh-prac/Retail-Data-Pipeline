from scripts.extract import extract_user_purchase, extract_movie_review


DATA_DIR = 'data/'

def main():
    print("Starting ETL process...\n")


    user_df = extract_user_purchase(DATA_DIR)
    movie_df = extract_movie_review(DATA_DIR)

    print("Preview of user purchases:")

    print(user_df.head())

    print("\nPreview of movie reviews:")
    print(movie_df.head())

    print("\nETL process completed successfully!")


if __name__ == "__main__":
    main()