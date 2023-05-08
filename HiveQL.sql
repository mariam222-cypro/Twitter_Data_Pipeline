CREATE DATABASE IF NOT EXISTS wartweets;

SET hive.exec.dynamic.partition.mode=nonstrict;

use wartweets;

CREATE EXTERNAL TABLE IF NOT EXISTS landing_data (
 created_at string,
            text string,
            likes int,
            retweet_count int,
            impression_count int,
            tweet_id string,
            user_id string,
            user_followers_count int,
            user_name string,
            Full_UserName string,
            user_location string,
            user_verified boolean,
            country_code string,
            lang string,
            country_name string,
            city string
            )
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/data/twitter_landing_data/';

MSCK REPAIR TABLE landing_data;

--tweet_text_raw dimension table



CREATE EXTERNAL TABLE IF NOT EXISTS wartweets.tweet_text_raw (
    user_id STRING,
    tweet_id STRING,
    likes INT,
    text STRING
    
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/data/twitter_raw_data/tweet_text_raw';


INSERT overwrite TABLE wartweets.tweet_text_raw PARTITION (year, month, day, hour)
SELECT user_id,tweet_id, likes, text , year, month, day, hour
FROM landing_data;


-- time_dim_raw dimension table
CREATE EXTERNAL TABLE IF NOT EXISTS wartweets.time_dim_raw (
    tweet_id STRING,
    time_id INT,
    created_at STRING
    
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/data/twitter_raw_data/time_dim_raw';


INSERT OVERWRITE TABLE wartweets.time_dim_raw
PARTITION (year, month, day, hour)
SELECT
    tweet_id,
    ROW_NUMBER() OVER (ORDER BY created_at) AS time_id,
    created_at,
    year,
    month,
    day,
    hour
FROM landing_data;


-- users_raw dimension table
CREATE EXTERNAL TABLE IF NOT EXISTS wartweets.users_raw (
    user_id STRING,
    Full_UserName STRING,
    user_name STRING,
    user_location STRING,
    user_followers_count INT,
    user_verified BOOLEAN
    
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/data/twitter_raw_data/users_raw';

INSERT OVERWRITE TABLE wartweets.users_raw
PARTITION (year, month, day, hour)
SELECT user_id, Full_UserName, user_name, user_location, user_followers_count, user_verified,year, month, day, hour
FROM landing_data;


-- geographic_location_raw dimension table
CREATE EXTERNAL TABLE IF NOT EXISTS wartweets.geographic_location_raw (
    tweet_id STRING,
    country_code STRING,
    country_name STRING,
    city STRING
    
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/data/twitter_raw_data/geographic_location_raw';

INSERT OVERWRITE TABLE wartweets.geographic_location_raw
PARTITION (year, month, day, hour)
SELECT tweet_id, country_code, country_name, city, year, month, day, hour
FROM landing_data ;


-- sentiment_category_raw dimension table
CREATE EXTERNAL TABLE IF NOT EXISTS wartweets.sentiment_dimension (
    sentiment_category STRING,
    sentiment_score_Min FLOAT,
    sentiment_score_Max FLOAT
)
STORED AS PARQUET
LOCATION '/data/twitter_raw_data/sentiment_dimension';

INSERT INTO wartweets.sentiment_dimension VALUES ('Positive', 0.5, 1.0);
INSERT INTO wartweets.sentiment_dimension VALUES ('Neutral', 0.0, 0.5);
INSERT INTO wartweets.sentiment_dimension VALUES ('Negative', -1.0, 0.0);

-- influential_categories_raw dimension table
CREATE EXTERNAL TABLE IF NOT EXISTS wartweets.influential_categories_raw (followers_count_Min INT,followers_count_Max INT,influential_category STRING)
STORED AS PARQUET
LOCATION '/data/twitter_raw_data/influential_categories_raw';

INSERT INTO wartweets.influential_categories_raw VALUES (0, 1000, 'Nano Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (1001, 10000, 'Micro Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (10001, 100000, 'Macro Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (100001, 1000000, 'Mega Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (1000001, 5000000, 'Super Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (5000001, 10000000, 'Hyper Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (10000001, 50000000, 'Power Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (50000001, 100000000, 'Mighty Influencer');
INSERT INTO wartweets.influential_categories_raw VALUES (100000001, 300000000, 'Giga Influencer');
