from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from pyspark.sql.functions import countDistinct, sum, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_extract, regexp_replace
from textblob import TextBlob


spark = SparkSession.builder \
        .appName("TwitterFactTable") \
        .enableHiveSupport() \
        .getOrCreate()
spark.conf.set("hive.metastore.uris", "thrift://localhost:5432")
spark.conf.set("spark.sql.sources.default", "parquet")




tweet_text_raw_view = spark.sql("select * from wartweets.tweet_text_raw")
time_dim_raw_view = spark.sql("select * from wartweets.time_dim_raw")
users_raw_view = spark.sql("select * from wartweets.users_raw")
geographic_location_raw_view = spark.sql("select * from wartweets.geographic_location_raw")
sentiment_dimension_view = spark.sql("select * from wartweets.sentiment_dimension")
influential_categories_raw_view = spark.sql("select * from wartweets.influential_categories_raw")

def calculate_sentiment(text):
	
    blob = TextBlob(text)
	
    if blob.detect_language() != 'en':
        return None
	
    return blob.sentiment.polarity   

# calculate the sentiment score using the UDF
spark.udf.register("calculate_sentiment", calculate_sentiment, FloatType())

tweet_text_raw_view = tweet_text_raw_view.withColumn("sentiment_score", udf(lambda x: calculate_sentiment(x), FloatType())("text"))

# Extract hashtags from the text column and create a new column for it
tweet_text_raw_view = tweet_text_raw_view.withColumn("hashtags", regexp_extract(col("text"), r"#(\w+)", 1))

# Remove links from the text column and update the column in-place
tweet_text_raw_view = tweet_text_raw_view.withColumn("text", regexp_replace(col("text"), r"http\S+", ""))

# Remove mentions from the text column and update the column in-place
tweet_text_raw_view = tweet_text_raw_view.withColumn("text", regexp_replace("text", "@\\w+", ""))

# Remove extra spaces from the text column
tweet_text_raw_view = tweet_text_raw_view.withColumn("text", regexp_replace(col("text"), r"\s+", " "))

#Track the spread of hashtags related to the conflict
tweet_text_raw_view_new = tweet_text_raw_view.select('tweet_id', 'hashtags')

hashtag_spread_fact = (
    tweet_text_raw_view_new
    .join(time_dim_raw_view, "tweet_id")
    .groupBy("hashtags", "year", "month", "day", "hour")
    .agg(count("tweet_id").alias("tweet_count"))
    .orderBy("year", "month", "day", "hour")
)


# Analyze sentiment towards the conflict among Twitter users
sentiment_analysis_fact = (
	tweet_text_raw_view.alias("tdr")
	.join(sentiment_dimension_view.alias("sd"), (col("tdr.sentiment_score") >= col("sd.sentiment_score_min")) & (col("tdr.sentiment_score") <= col("sd.sentiment_score_max")))
	.groupBy( "sd.sentiment_category")
	.agg(count("tdr.tweet_id").alias("tweet_count"))
	.select("sd.sentiment_category", "tweet_count")
)

# Identify influential users or accounts discussing the topic.
influential_users_fact = (
		users_raw_view
		.join(influential_categories_raw_view, 
					(col("user_followers_count") >= col("followers_count_min")) &
					(col("user_followers_count") <= col("followers_count_max")))
		.select("user_name", "influential_category", "user_followers_count")
)

#Identifing Top 10 influential users 
Top_10_Inf_Fact = (
	tweet_text_raw_view
	.join(users_raw_view, 'user_id')
	.where('user_followers_count >= 10000')
	.groupBy('user_id', 'Full_UserName', 'user_location', 'user_followers_count')
	.agg(countDistinct('tweet_id').alias('tweet_count'))
	.orderBy(desc('tweet_count'))
	.limit(10)
)

# Analyze the geographic location of Twitter users discussing the conflict
geographic_location_fact = (
	users_raw_view
	.join(tweet_text_raw_view_new, 'user_id','inner')
	.groupBy('user_location')
	.agg(count('tweet_id').alias('tweet_count'))
    .orderBy(desc('tweet_count'))
)


# Analyze the volume and sentiment of tweets related to the conflict over time

tweet_text_raw_view_new=tweet_text_raw_view.select('user_id','sentiment_score','likes','tweet_id')
tweet_count_fact = (
	tweet_text_raw_view_new
	.join(time_dim_raw_view, 'tweet_id')
	.groupBy('year', 'month', 'day', 'hour')
	.agg(
		countDistinct('tweet_id').alias('tweet_count'),
		sum('likes').alias('total_likes'),
		avg('sentiment_score').alias('avg_sentiment_score'),
		countDistinct('user_id').alias('unique_users_count')
	))



hashtags_exploded = tweet_text_raw_view.select(explode(split("hashtags", ",\s*")).alias("hashtag"))
hashtag_df = hashtags_exploded.select(lower("hashtag").alias("hashtag"))

# Calculate the count of each hashtag
hashtag_count_df = hashtag_df.groupBy("hashtag").count().orderBy("count", ascending=False)

# Calculate the influential categories of each 

tweet_count_by_category = (
	tweet_text_raw_view
	.join(users_raw_view, tweet_text_raw_view.user_id == users_raw_view.user_id)
	.join(influential_categories_raw_view, (users_raw_view.user_followers_count >= influential_categories_raw_view.followers_count_Min) & 
		(users_raw_view.user_followers_count <= influential_categories_raw_view.followers_count_Max))
	.join(sentiment_dimension_view, (tweet_text_raw_view.sentiment_score >= sentiment_dimension_view.sentiment_score_Min) & 
		(tweet_text_raw_view.sentiment_score <= sentiment_dimension_view.sentiment_score_Max))
	.groupBy('sentiment_category', 'influential_category')
	.agg(count('*').alias('tweet_count'))
	.orderBy('sentiment_category', 'influential_category')
)


user_sentiment_category = (
	tweet_text_raw_view
	.join(users_raw_view, tweet_text_raw_view.user_id == users_raw_view.user_id)
	.join(sentiment_dimension_view, (tweet_text_raw_view.sentiment_score >= sentiment_dimension_view.sentiment_score_Min) & 
		(tweet_text_raw_view.sentiment_score <= sentiment_dimension_view.sentiment_score_Max))
	.join(influential_categories_raw_view, (users_raw_view.user_followers_count >= influential_categories_raw_view.followers_count_Min) & 
		(users_raw_view.user_followers_count <= influential_categories_raw_view.followers_count_Max))
	.groupBy(users_raw_view.user_id, 'sentiment_category', 'influential_category')
	.agg(count('tweet_id').alias('tweet_count'))
)


#Save each fact table as a Spark SQL table
spark.sql("CREATE DATABASE IF NOT EXISTS twitter_processed_data2")

hashtag_spread_fact.write.mode('overwrite').saveAsTable('twitter_processed_data2.hashtag_spread_fact_processed')

sentiment_analysis_fact.write.mode("overwrite").saveAsTable('twitter_processed_data.sentiment_analysis_fact_processed')

influential_users_fact.write.mode("overwrite").saveAsTable('twitter_processed_data.influential_users_fact_processed')

Top_10_Inf_Fact.write.mode("overwrite").saveAsTable('twitter_processed_data.Top_10_Inf_Fact_processed')

geographic_location_fact.write.mode("overwrite").saveAsTable('twitter_processed_data.geographic_location_fact_processed')

tweet_count_fact.write.mode("overwrite").saveAsTable('twitter_processed_data.tweet_count_fact_processed')

hashtag_count_df.write.mode("overwrite").saveAsTable('twitter_processed_data.hashtag_count_df_processed')

tweet_count_by_category.write.mode("overwrite").saveAsTable('twitter_processed_data.tweet_count_by_category_processed')

user_sentiment_category.write.mode("overwrite").saveAsTable('twitter_processed_data.user_sentiment_category_processed')