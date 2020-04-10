import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.spark.streaming.dstream.InputDStream

object Consumer extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val topic = Array("parking_ticket_violation")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream_id",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val kafkaStreams:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topic, kafkaParams)
  )

  kafkaStreams.map(record => (record.value)).foreachRDD( rdd => {
    if(!rdd.isEmpty())
    {

      // Save all violation in a text file
      rdd.saveAsTextFile("/home/tgaullier/Documents/Functional_Data_Programming/fdp_project/src/main")
      rdd.map(x => {
        val split = x.split(',')

        // Search for a potential violation with a code "unknown"
        if(split(3).toString == "\"violation_code\":\"unknown\"") {
          val unknown_violation = "NEW ALERT : " + split.mkString(",")
          println(unknown_violation)
        }

      }).foreach(print)


    }
  })

  ssc.start()
  ssc.awaitTermination()

}