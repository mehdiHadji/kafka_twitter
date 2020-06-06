import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta

#Use your creds
consumer_key        = "############"
consumer_secret     = "############"
access_token        = "############"
access_token_secret = "############"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


# Formatting time to GMT+1 since the tweets are timestamped in GMT timezone
# This also a good exercise to do something with data before sending it
def normalize_timestamp(time):
	mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
	mytime += timedelta(hours=1)
	return (mytime.strftime("%Y-%m-%d %H:%M:%S"))


# kafka producer
producer   = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'tweets-lambda1'


def get_twitter_data():
    res = api.search("covid19 morocco OR corona maroc OR confinement maroc OR corona morocco")
    for i in res:
        record = ''
        record += str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.user.followers_count)
        record += ';'
        record += str(i.user.location)
        record += ';'
        record += str(i.favorite_count)
        record += ';'
        record += str(i.retweet_count)
        record += ';'
        print(record)
        producer.send(topic_name, str.encode(record))

def periodic_work(interval):
	while True:
		get_twitter_data()
		time.sleep(interval)


periodic_work(60 * 0.1)
