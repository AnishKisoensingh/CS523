2 pieces to this project

1.
Spark streaming integrated with Hbase. This is achieved through happybase which requires thrift to be running with the following command
hbase thrift start
Once thrift is running, run the producer program and the consumer program. These programs will populate the data in HBase, I have created
the table before hand using the command
hbase shell
create 'popularMobile', 'infeed'

2.
The second piece to this project is Kafka, which has uses twitter streaming API to send messages over from the producer to consumer. The topic
name is mobiles for my application.

url to video
https://web.microsoftstream.com/video/492e7b23-fc1e-4189-b09b-b0303dc7680c