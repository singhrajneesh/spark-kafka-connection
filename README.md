#spark-kafka-connection
    This code contains the method to create the connection between the Kafka and Spark.
    Code contains the two ways of the connection between the kafka and spark 
    1. Direct Stream method
    2. Structured Streaming method


#### **Command to start kafka**
    1. Start Zookeeper
    C:/Users/r289/Downloads/kafka/bin/windows/zookeeper-server-start.bat C:/Users/r289/Downloads/kafka/config/zookeeper.properties
    2. Start Broker
    C:/Users/r289/Downloads/kafka/bin/windows/kafka-server-start.bat C:/Users/r289/Downloads/kafka/config/server.properties
    3. Create a topic
    C:/Users/r289/Downloads/kafka/bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test1
    4. List the topics
    C:/Users/r289/Downloads/kafka/bin/windows/kafka-topics.bat --list --zookeeper localhost:2181
    5. Start kafka producer
    C:/Users/r289/Downloads/kafka/bin/windows/kafka-console-producer.bat --broker-list localhost:9092 --topic test1
    6. Start consumer for the kafka
    C:/Users/r289/Downloads/kafka/bin/windows/kafka-console-consumer.bat --zookeeper localhost:2181 —topic test1 --from-beginning
 
 Note:- Document and configs are present in the resource folders