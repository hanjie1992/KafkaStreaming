import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  *  kafkaStreaming 实时流接入处理
  */
object KafkaStreaming {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
        .builder()
        .master("local[6]")
        .appName("KafkaStreaming")
        .getOrCreate()

    val sparkContext: SparkContext = spark.sparkContext
    val sparkStreaming = new StreamingContext(sparkContext,Seconds(5))
    sparkStreaming.sparkContext.setLogLevel("ERROR")

    val kafkaParams: Map[String, Object] = Map[String,Object](
      "bootstrap.servers" -> "192.168.1.71:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("wangtianxiong")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](
      sparkStreaming,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )

    val numberCounts: DStream[(String, Long)] = stream.map(x => x.value()).flatMap(_.split(" ")).map(x => (x,1L)).reduceByKey(_+_)
    numberCounts.print()
    sparkStreaming.start()
    sparkStreaming.awaitTermination()

  }

}
