import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
from dotenv import load_dotenv

load_dotenv()

topic_name = 'twitterdata'
output_topic = 'twitterdata-clean'


class Sentiment:




    @staticmethod
    def preprocessing(df):
        # words = df.select(explode(split(df.text, " ")).alias("word"))
        df = df.filter(col('text').isNotNull())
        df = df.withColumn('text', regexp_replace('text', r'http\S+', ''))
        df = df.withColumn('text', regexp_replace('text', r'[^\x00-\x7F]+', ''))
        df = df.withColumn('text', regexp_replace('text', r'[\n\r]', ' '))
        df = df.withColumn('text', regexp_replace('text', '@\w+', ''))
        df = df.withColumn('text', regexp_replace('text', '#', ''))
        df = df.withColumn('text', regexp_replace('text', 'RT', ''))
        df = df.withColumn('text', regexp_replace('text', ':', ''))
        df = df.withColumn('source', regexp_replace('source', '<a href="' , ''))

        return df

    # text classification
    @staticmethod
    def polarity_detection(text):
        return TextBlob(text).sentiment.polarity

    @staticmethod
    def subjectivity_detection(text):
        return TextBlob(text).sentiment.subjectivity

    @staticmethod
    def text_classification(words):
        # polarity detection
        polarity_detection_udf = udf(Sentiment.polarity_detection, FloatType())
        words = words.withColumn("polarity_v", polarity_detection_udf("text"))
        words = words.withColumn(
            'polarity',
            when(col('polarity_v') > 0, lit('Positive'))
            .when(col('polarity_v') == 0, lit('Neutral'))
            .otherwise(lit('Negative'))
        )
        # subjectivity detection
        subjectivity_detection_udf = udf(Sentiment.subjectivity_detection, FloatType())
        words = words.withColumn("subjectivity_v", subjectivity_detection_udf("text"))
        return words


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession \
        .builder \
        .appName("tweeter tweets") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.3.5") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Ignore INFO DEBUG output
    df1 = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", topic_name) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \

    schema = StructType() \
        .add("created_at", TimestampType(), True) \
        .add("id", StringType(), True) \
        .add("text", StringType(), True) \
        .add("source", StringType(), True) \
        .add("truncated", StringType(), True) \
        .add("in_reply_to_status_id", StringType(), True) \
        .add("in_reply_to_user_id", StringType(), True) \
        .add("in_reply_to_screen_name", StringType(), True) \
        .add("user", StringType(), True) \
        .add("coordinates", StringType(), True) \
        .add("place", StringType(), True) \
        .add("quoted_status_id", StringType(), True) \
        .add("is_quote_status", StringType(), True) \
        .add("quoted_status", StringType(), True) \
        .add("retweeted_status", StringType(), True) \
        .add("quote_count", StringType(), True) \
        .add("reply_count", StringType(), True) \
        .add("retweet_count", StringType(), True) \
        .add("favorite_count", StringType(), True) \
        .add("entities", StringType(), True) \
        .add("extended_entities", StringType(), True) \
        .add("favorited", StringType(), True) \
        .add("retweeted", StringType(), True) \
        .add("possibly_sensitive", StringType(), True) \
        .add("filter_level", StringType(), True) \
        .add("lang", StringType(), True) \
        .add("matching_rules", StringType(), True) \
        .add("name", StringType(), True) \
        .add("timestamp_ms", StringType(),True)


    details_schema2 = df1.select(from_json(col('value'), schema).alias("json"))
    details_schema2 = details_schema2.selectExpr("json.*") \


        # Preprocess the data
    df= Sentiment.preprocessing(details_schema2)

    # text classification to define polarity and subjectivity
    df2 = Sentiment.text_classification(df) \
    .selectExpr('timestamp_ms', 'id', 'text', 'polarity_v', 'polarity', 'subjectivity_v') \
    .withColumn("ts", to_timestamp(from_unixtime(expr("timestamp_ms/1000")))) \
    .withWatermark("ts", "1 second")
    df2.printSchema()

    df2.createOrReplaceTempView("details")

    result = spark.sql("""
      SELECT   id,
                window.start AS start_timestamp,
                window.end AS end_timestamp,
                COUNT(*) AS num_observations_in_window,
                 text, 
                 polarity_v,
                  polarity, 
                  subjectivity_v

      FROM      details AS t
      GROUP BY  id,text,polarity_v, polarity, subjectivity_v ,WINDOW(t.ts , '10 Second')



      """);



query = result\
        .writeStream \
        .outputMode('append') \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .trigger(processingTime="10 seconds") \
        .start()

db_url = "jdbc:postgresql://localhost:25432/db"
db_driver = "org.postgresql.Driver"
db_username = "user"
db_password = "user"

def write_batch_to_db(batch_dataframe: DataFrame, batch_id):
    # note use of mode 'append' that preserves existing data in the table (alternative 'overwrite')
    print(f"writing {batch_dataframe.count()} rows to {db_url}")
    batch_dataframe \
        .write \
        .format("jdbc") \
        .mode("append") \
        .option("url", db_url) \
        .option("driver", db_driver) \
        .option("user", db_username) \
        .option("password", db_password) \
        .option("dbtable", "data") \
        .option("truncate", False) \
        .save()

result.writeStream \
    .foreachBatch(write_batch_to_db) \
    .outputMode("append") \
    .start() \
    .awaitTermination()