import org.apache.spark.sql.{Encoders, SparkSession}
import java.time.{LocalDate, Period}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object SparkStreamingKafkaStructuredStreaming extends App {



  val spark=SparkSession.builder().appName("StructuredStreaming")
    .master("local[*]").getOrCreate()

  //input data source
  //{"firstName":"Quentin","lastName":"Corkery","birthDate":"1984-10-26T03:52:14.449+0000"}
  //{"firstName":"Neil","lastName":"Macejkovic","birthDate":"1971-08-06T18:03:11.533+0000"}

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test1")
    // Subscribe to multiple topics
    //.option("subscribe", "topic1,topic2")
    // Subscribe to 1 topic, with headers
    //.option("includeHeaders", "true")
    // Subscribe to a pattern
    //.option("subscribePattern", "topic.*")
    .load()


  val dataPerson=df.selectExpr("CAST(value AS STRING)")

  val struct = new StructType()
    .add("firstName", DataTypes.StringType)
    .add("lastName", DataTypes.StringType)
    .add("birthDate", DataTypes.StringType)

  val result=dataPerson.select(from_json(col("value"),struct)).as("person")

  val consoleOutput=result.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()

  consoleOutput.awaitTermination()

}
