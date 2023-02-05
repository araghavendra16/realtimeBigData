Real time big data project: 

The project’s main aim is building a real-time big data application - Sentiment analysis of real-time tweet data on FIFA WORLD CUP.
We build a big data pipeline which can handle the streaming data.

The application proposed will retrieve data from Twitter (microblogging
website) in form of tweets. We use Apache Kafka producer to store the streaming
tweets and is then consumed by Apache Spark. Using Textblob, sentiments of
tweets are computed. The results is stored in Postgres and dis-played as a
dashboard using Grafana.

![arch](https://user-images.githubusercontent.com/61226849/216811825-f4e31984-c941-4002-ba70-972ec04cb780.png)



Python packages installation requirements: 

pip install tweepy
pip install 'kafka-python' 
pip install pyspark 
pip install TextBlob 


Run 'docker compose up'

Run 'producer.py'
Run 'spark.py'

PostgreSQL: 
Open http://localhost:23000/ (pgadmin)
login with default credentials user:user



