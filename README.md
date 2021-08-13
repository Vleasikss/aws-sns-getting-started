sbt compile

sbt run

Difference between Apache Kafka && SQS/SNS

The use-cases for Kafka and Amazon SQS/Amazon SNS are quite different.


Kafka is a distributed publish-subscribe system. It is designed for very high throughput, processing thousands of messages per second. Of course you need to setup and cluster it for yourself. It supports multiple readers, which may "catch up" with the stream of messages at any point (well, as long as the messages are still on disk). You can use it both as a queue (using consumer groups) and as a topic.

An important characteristic is that you cannot selectively acknowledge messages as "processed"; the only option is acknowledging all messages up to a certain offset.

SQS/SNS on the other hand:

- no setup/no maintenance
- either a queue (SQS) or a topic (SNS)
- various limitations (on size, how long a message lives, etc) 
- limited throughput: you can do batch and concurrent requests, but still achieving high throughputs would be expensive

I'm not sure if the messages are replicated; however at-least-once guarantee delivery in SQS would suggest so
SNS has notifications for email, SMS, SQS, HTTP built-in. With Kafka, you would probably have to code it yourself
no "message stream" concept
So overall I would say SQS/SNS are well suited for simpler tasks and workloads with a lower volume of messages.

Share
