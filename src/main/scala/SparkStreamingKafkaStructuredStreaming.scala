import org.apache.spark.sql.SparkSession

object SparkStreamingKafkaStructuredStreaming extends App {
  val spark=SparkSession.builder().appName("StructuredStreaming")
    .master("local[*]").getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribe", "test1")
    // Subscribe to multiple topics
    //.option("subscribe", "topic1,topic2")
    // Subscribe to 1 topic, with headers
    .option("includeHeaders", "true")
    // Subscribe to a pattern
    //.option("subscribePattern", "topic.*")
    .load()
  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
    .asInstanceOf[(String, String, Array[(String, Array[Byte])])]

  df.show(100,false)

}
