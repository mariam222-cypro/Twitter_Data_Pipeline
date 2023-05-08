# Twitter_Data_Pipeline
Real Time Streaming: Twitter Data Pipeline Using Big data Tools

# Twitter Data Pipeline Using Big Data Tools

## Introduction

This project aims to analyze Twitter data related to the conflict between Ukraine and Russia in five main angles over time:

1. Analyze sentiment towards the conflict among Twitter users.
2. Track the spread of hashtags or keywords related to the conflict.
3. Identify influential users or accounts discussing the topic.
4. Analyze the volume and sentiment of tweets related to the conflict over time.
5. Analyze the geographic location of Twitter users discussing the conflict.

The pipeline consists of several stages, including data collection, landing data persistence, landing to raw ETL, raw to processed ETL, and a shell script coordinator.

## Architecture Details

The architecture of the pipeline consists of several stages that work together to collect, process, and analyze the Twitter data. Here's a brief overview of each stage:

### Data Collection System
This stage is responsible for collecting data from the Twitter API and storing it in HDFS partitioned by the year, month, day, and hour of the tweet's creation. The data collector is a long-running job that receives data from a port opened in the previous stage of the pipeline.

### Landing Data Persistence
This stage stores the collected data in its base format in HDFS partitioned by the year, month, day, and hour of the tweet's creation. The data is stored as Parquet, and a Hive table is created on top of the directory for use in later stages of the pipeline. Finally, the data is stored in an external Hive table named landing_data and stores it in HDFS.

### Landing to Raw ETL
This stage runs HiveQL queries to extract dimensions from the landing data. The code creates several dimension tables, which are used to categorize and organize data. The tables created include tweet_text_raw, time_dim_raw, users_raw, geographic_location_raw, sentiment_category_raw, and influential_categories_raw.

### Raw to Processed ETL
This stage uses PySpark to process the raw data stored in HDFS and generate a processed dataset. The processed dataset is stored in HDFS as Parquet and is used for further analysis.

### Shell Script Coordinator
This stage consists of a shell script that coordinates the execution of the previous stages of the pipeline.

## Getting Started

To get started with the project, you'll need to follow these steps:

Install the necessary dependencies. This project uses PySpark, Hive, and Hadoop. Make sure you have them installed and configured on your system.
Clone the repository to your local machine.
Create a Twitter developer account and obtain the necessary credentials to access the Twitter API.
Update the config.py file with your Twitter API credentials and other relevant settings.
Run the shell script coordinator to execute the pipeline.

## Contributing

If you'd like to contribute to the project, feel free to submit a pull request. Please make sure to follow the project's coding style and guidelines.

