from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json
from datetime import datetime
from textblob import TextBlob
from pyspark.sql import HiveContext
import datetime
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import col, get_json_object, year, month, dayofmonth, hour, from_utc_timestamp
import time

spark = SparkSession.builder \
        .appName("TwitterDataCollectionSystem") \
        .enableHiveSupport() \
        .getOrCreate()
spark.conf.set("hive.metastore.uris", "thrift://localhost:5432")
spark.conf.set("spark.sql.sources.default", "parquet")

# create schema for the tweets
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType,TimestampType,IntegerType,StringType,ArrayType,FloatType
from pyspark.sql.functions import udf, struct

from pyspark.sql.functions import from_json, col

stream = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load()

def extract_tweet_data(tweet_df):
    output_df = tweet_df.selectExpr("value as raw_tweet")
    output_df = output_df.withColumn("created_at", from_utc_timestamp(get_json_object(col("raw_tweet"), "$.created_at"), "UTC"))
    output_df = output_df.withColumn("year", year("created_at"))
    output_df = output_df.withColumn("month", month("created_at"))
    output_df = output_df.withColumn("day", dayofmonth("created_at"))
    output_df = output_df.withColumn("hour", hour("created_at"))
    output_df = output_df.withColumn("text", get_json_object(col("raw_tweet"), "$.text"))
    output_df = output_df.withColumn("likes", get_json_object(col("raw_tweet"), "$.public_metrics.like_count"))
    output_df = output_df.withColumn("retweet_count", get_json_object(col("raw_tweet"), "$.public_metrics.retweet_count"))
    output_df = output_df.withColumn("impression_count", get_json_object(col("raw_tweet"), "$.public_metrics.impression_count"))
    output_df = output_df.withColumn("tweet_id", get_json_object(col("raw_tweet"), "$.id"))
    output_df = output_df.withColumn("user_id", get_json_object(col("raw_tweet"), "$.author_id"))
    output_df = output_df.withColumn("user_followers_count", get_json_object(col("raw_tweet"), "$.user.public_metrics.followers_count"))
    output_df = output_df.withColumn("user_name", get_json_object(col("raw_tweet"), "$.user.username"))
    output_df = output_df.withColumn("Full_UserName", get_json_object(col("raw_tweet"), "$.user.name"))
    output_df = output_df.withColumn("user_location", get_json_object(col("raw_tweet"), "$.user.location"))
    output_df = output_df.withColumn("user_verified", get_json_object(col("raw_tweet"), "$.user.verified"))
    output_df = output_df.withColumn("country_code", get_json_object(col("raw_tweet"), "$.place.country_code"))
    output_df = output_df.withColumn("lang", get_json_object(col("raw_tweet"), "$.lang"))
    output_df = output_df.withColumn("country_name", get_json_object(col("raw_tweet"), "$.place.country"))
    output_df = output_df.withColumn("city", get_json_object(col("raw_tweet"), "$.place.full_name"))
    return output_df
  
from pyspark.sql.functions import col

output_df=extract_tweet_data(stream) 
output_df = output_df \
    .withColumn("created_at", col("created_at").cast("string")) \
    .withColumn("year", col("year").cast("integer")) \
    .withColumn("month", col("month").cast("integer")) \
    .withColumn("day", col("day").cast("integer")) \
    .withColumn("hour", col("hour").cast("integer")) \
    .withColumn("text", col("text").cast("string")) \
    .withColumn("likes", col("likes").cast("integer")) \
    .withColumn("retweet_count", col("retweet_count").cast("integer")) \
    .withColumn("impression_count", col("impression_count").cast("integer")) \
    .withColumn("tweet_id", col("tweet_id").cast("string")) \
    .withColumn("user_id", col("user_id").cast("string")) \
    .withColumn("user_followers_count", col("user_followers_count").cast("integer")) \
    .withColumn("user_name", col("user_name").cast("string")) \
    .withColumn("Full_UserName", col("Full_UserName").cast("string")) \
    .withColumn("user_location", col("user_location").cast("string")) \
    .withColumn("user_verified", col("user_verified").cast("boolean")) \
    .withColumn("country_code", col("country_code").cast("string")) \
    .withColumn("lang", col("lang").cast("string")) \
    .withColumn("country_name", col("country_name").cast("string")) \
    .withColumn("city", col("city").cast("string"))

output_df.writeStream. \
    outputMode("append"). \
    format("memory"). \
    queryName("tweetquery"). \
    trigger(processingTime='2 seconds'). \
    start()

time.sleep(60)

df = spark.sql("select * from tweetquery")

df.write.mode("append").partitionBy("year", "month", "day", "hour").parquet("/data/twitter_landing_data/")

