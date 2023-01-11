

from dotenv import load_dotenv
import tweepy
from kafka import KafkaProducer
import logging
import os

load_dotenv()
os.environ['consumer_key1'] = "nJxZXgso2ZjC2YQzmpt1VCAWO"
consumer_key = os.environ.get('consumer_key1')

os.environ['consumer_secret1'] = 'eAmswQtRGG61KCZdTCuBGSdcsO4HHs8m3WY9D0BQOKSh8az0XX'
consumer_secret = os.environ.get('consumer_secret1')


os.environ['access_token1'] = '736888522636984320-ifdixndq2Q8m521ZreLF8ybnSEGgsWO'
access_tokens = os.environ.get('access_token1')

os.environ['access_token_secret1'] = 'cjIzzuDxXi1uGrnhgBT0rJV6k474qObjf0yMLWw91JHvr'
access_tokens_secret = os.environ.get('access_token_secret1')

logger = logging.getLogger(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:29092')

topic_name = "twitterdata"
search_input = "fifa, #FIFA, FIFA, #fifa, #ArgentinaVsFrance, #FIFAWorldCup2022"


class TwitterStreamer():
    def stream_data(self):
        logger.info(f"{topic_name} Stream starting for {search_input}...")

        twitter_stream = MyListener(consumer_key, consumer_secret, access_tokens, access_tokens_secret)
        twitter_stream.filter(track=[search_input])


class MyListener(tweepy.Stream):
    def on_data(self, data):
        try:
            logger.info(f"Sending data to kafka...")
            producer.send(topic_name, data)
            print(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_data()