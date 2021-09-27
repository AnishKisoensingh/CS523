#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
import json


# In[2]:


topic_name = "mobiles"


# In[3]:


consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms =  5000,
    fetch_max_bytes = 128,
    max_poll_records = 100,
    api_version=(0, 10, 1),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# In[ ]:


for message in consumer:
 tweets = json.loads(json.dumps(message.value))
 print(tweets)

