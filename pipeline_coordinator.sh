#!/bin/bash

# Twitter API Script
python3 /home/itversity/itversity-material/mariam/get_tweets.py &

/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit /home/itversity/itversity-material/mariam/read_tweet.py 

# HiveQL Script(s)
/opt/spark2/bin/spark-sql -f /home/itversity/itversity-material/mariam/HiveQL.sql

# SparkSQL App
/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit /home/itversity/itversity-material/mariam/SparkSqlApp.py


