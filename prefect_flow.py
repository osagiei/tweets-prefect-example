from prefect import flow, task
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col, max

@task
def load_data(tweets_path: str, users_path: str, cities_path: str):
    spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()

    tweets_df = spark.read.json(tweets_path + "/*.json")
    users_df = spark.read.json(users_path)
    cities_df = spark.read.json(cities_path)

    return tweets_df, users_df, cities_df


@task
def process_data(
        tweets_df: DataFrame,
        users_df: DataFrame,
        cities_df: DataFrame):
    tweets_per_user_df = tweets_df.groupBy(
        "user.screen_name").agg(count("*").alias("tweet_count"))
    tweets_per_user_df = tweets_per_user_df.orderBy(col("tweet_count").desc())
    max_followers_user_df = tweets_df.groupBy("user.screen_name").agg(
        max("user.followers_count").alias("max_followers")).orderBy(
        col("max_followers").desc()).limit(1)
    tweets_with_city_df = tweets_df.join(
        users_df, tweets_df["user.screen_name"] == users_df["screen_name"], "inner")
    tweets_with_country_df = tweets_with_city_df.join(
        cities_df, tweets_with_city_df["city"] == cities_df["cities"], "inner")
    followers_increase_by_country_df = tweets_with_country_df.groupBy("countries").agg(
        max("user.followers_count").alias("max_followers")).orderBy(
        col("max_followers").desc()).limit(1)

    return tweets_per_user_df, max_followers_user_df, followers_increase_by_country_df


@task
def output_results(
        tweets_per_user_df: DataFrame,
        max_followers_user_df: DataFrame,
        followers_increase_by_country_df: DataFrame):
    print("\nTop 10 Users with Most Tweets:\n")
    tweets_per_user_df.show(10)

    print("\nUser with Maximum Followers:\n")
    max_followers_user_df.show()

    print("\nCountry with Maximum Increase in Followers:\n")
    followers_increase_by_country_df.show()


@flow
def twitter_analysis_flow(
        tweets_path: str = "/app/data/data-tweets/tweets/",
        users_path: str = "/app/data/data-tweets/master/users.json",
        cities_path: str = "/app/data/data-tweets/master/cities.json"):
    tweets_df, users_df, cities_df = load_data(
        tweets_path, users_path, cities_path)
    tweets_per_user_df, max_followers_user_df, followers_increase_by_country_df = process_data(
        tweets_df, users_df, cities_df)
    output_results(
        tweets_per_user_df,
        max_followers_user_df,
        followers_increase_by_country_df)


if __name__ == "__main__":
    twitter_analysis_flow()
