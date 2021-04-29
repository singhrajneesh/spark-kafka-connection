import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreamingKafkaConnection extends App {

  val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
  val streamingContext = new StreamingContext(sparkConf, Seconds(10))
  val sparkContext = SparkSession.builder().appName("DirectKafkaWordCount").master("local[*]").getOrCreate()
  val Array(zkQuorum, consumerGroup) = Array("localhost:9092", "use_a_separate_group_id_for_each_stream")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> zkQuorum,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> consumerGroup,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //val topics = Array("test1", "test2")
  val topics = Array("test1")
  streamingContext.sparkContext.setLogLevel("ERROR")
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  // var lines = stream.map(record => record.value)


  stream.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    println("this is the value rdd contains " + rdd.toString())
    if (!rdd.isEmpty()) {
      println("rdd is not empty")
      rdd.foreach(consumerRecord => {
          println("partition is not empty")
          println("this is the value consumerRecord has " + consumerRecord.value())
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          val words = consumerRecord.value().split(" ").flatMap(_.split(" "))
          val pairs = words.map(word => (word, 1))
          pairs.map(println)
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    else {
      println("rdd is empty")
    }
  }


  streamingContext.start()
  streamingContext.awaitTermination()


}
