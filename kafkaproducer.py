#!/usr/bin/env python
# coding: utf-8

# In[1]:


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic


# In[2]:


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              api_version=(0, 10, 1),
              value_serializer=lambda x: dumps(x).encode('utf-8'))
admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})


# In[3]:


topic_name = "mobiles"


# In[4]:


topic_list = []
topic_list.append(NewTopic(topic_name, 1, 1))
admin_client.create_topics(topic_list)


# In[5]:


access_token = "1441520955231117317-rs7NqY1Uc7ptu97ICudu3KZsQ3Zm9P"
access_token_secret =  "pZGB2d363gLLHB8ARsDvEI2jlpOVPHpiW4UeClL8gZlM8"
consumer_key =  "gFLiT2e9s1vPIyLORdVmhhLPa"
consumer_secret =  "pNAi8x5RXz34g58k66RRHolGIX2E05yihRtKjLgSWmLkUwQ6tB"


# In[6]:


class twitterAuth():

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth


# In[7]:


class TwitterStreamer():

    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=['iphone','pixel','samsung'], stall_warnings=True, languages= ["en"])


# In[8]:


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


# In[9]:


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()

