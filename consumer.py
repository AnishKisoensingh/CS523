#!/usr/bin/env python
# coding: utf-8

# In[1]:


from __future__ import print_function

import os
import sys

os.environ["SPARK_HOME"] = "/usr/spark2.4.3"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import happybase
import datetime

import io


# In[2]:


import operator
import time
from itertools import chain

from py4j.protocol import Py4JJavaError

from pyspark import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.util import rddToFileName, TransformFunction
from pyspark.rdd import portable_hash
from pyspark.resultiterable import ResultIterable


# In[3]:


connection = happybase.Connection('127.0.0.1',9090)

connection.open()


# In[4]:


table = connection.table('popularMobile')
print("connected to " +str(table))


# In[5]:


sc = SparkContext(appName = "StreamingTwitterAnalysis")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)


# In[6]:


socket_stream = ssc.socketTextStream("127.0.0.3",9999)


# In[7]:


lines = socket_stream.window( 20 )


# In[8]:


words = lines.flatMap( lambda text: text.split( " " ) ).filter( lambda word: word.lower().startswith("iphone") or  word.lower().startswith("pixel") or  word.lower().startswith("samsung")).map( lambda word: ( word.lower() , 1) ).reduceByKey( lambda a,b:a+b)


# In[9]:


counts_sorted_dstream = words.transform(lambda foo:foo.sortBy(lambda x:x[0].lower()).sortBy(lambda x:x[1],ascending=False))
print(counts_sorted_dstream)


# In[10]:


counts_sorted_dstream.foreachRDD( lambda x:table.put(str(datetime.datetime.now()), {'infeed:word':str(x)}))


# In[11]:


#for row in author_counts_sorted_dstream:
#table.put(str(datetime.datetime.now()), {'infeed:word':row[0]})
counts_sorted_dstream.pprint()


# In[12]:


ssc.start()


# In[13]:


ssc.awaitTermination()


# In[ ]:





# In[ ]:





# In[ ]:




