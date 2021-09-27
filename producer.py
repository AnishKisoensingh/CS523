#!/usr/bin/env python
# coding: utf-8

# In[1]:


import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import socket
import json


# In[2]:


access_token = "1441520955231117317-rs7NqY1Uc7ptu97ICudu3KZsQ3Zm9P"
access_token_secret =  "pZGB2d363gLLHB8ARsDvEI2jlpOVPHpiW4UeClL8gZlM8"
consumer_key =  "gFLiT2e9s1vPIyLORdVmhhLPa"
consumer_secret =  "pNAi8x5RXz34g58k66RRHolGIX2E05yihRtKjLgSWmLkUwQ6tB"


# In[3]:


class TweetsListener(StreamListener):
    def __init__(self,csocket):
        self.client_socket = csocket
    def on_data(self,data):
        try:
            msg = json.loads( data )
            print( msg['text'].encode('utf-8') )
            self.client_socket.send( msg['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Exception caused by: %s" % str(e))
            return True
    def on_error(self, status):
        print(status)
        return True


# In[4]:


def sendData(c_socket):
        auth = OAuthHandler(consumer_key,consumer_secret)
        auth.set_access_token(access_token,access_token_secret)
        twitter_stream = Stream(auth, TweetsListener(c_socket))
        twitter_stream.filter(track=['iphone','pixel','samsung'])


# In[5]:


#s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s = socket.socket()
host = "127.0.0.3"
port = 9999
s.bind((host, port))
print("Listening on port: %s" % str(port))


# In[6]:


s.listen(5)
c, addr = s.accept()
print( "Received request from: "+ str( addr ) )


# In[ ]:


sendData( c )


# In[ ]:





# In[ ]:




