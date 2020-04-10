import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, _}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.Json

object csvProducer extends App {
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)
  val topic = "parking_ticket_violation"
  implicit val messageWrites = Json.writes[Message]

  // Retrieves the csv file and goes through each line
  val bufferedSource = scala.io.Source.fromFile("/home/tgaullier/Documents/Functional_Data_Programming/fdp_project/Parking_Violations_Issued_-_Fiscal_Year_2015.csv").getLines().drop(1).foreach { line =>

    // Delimiter to "," and .trim clarifies the code
    val g_cols = line.split(",").map(_.trim)

    // Avoid error when there is less than 35 columns
    if (g_cols.size >= 35){

      // Retrieves the wanted columns
      val g_line = s"${g_cols(2)}" + "," + s"${g_cols(4)}" + "," + s"${g_cols(5)}" + "," + s"${g_cols(6)}" + "," + s"${g_cols(7)}" + "," + s"${g_cols(9)}" + "," + s"${g_cols(23)}" + "," + s"${g_cols(33)}" + "," + s"${g_cols(35)}" + ","

      // Avoid an empty column
      if (g_line.contains(",,") == false) {

        // Clear the data and pass it to the Consumer
        val cols = g_line.split(",").map(_.trim)
        val message = Message(drone_id = "-1", state = s"${cols(0)}", date = s"${cols(1)}", violation_code = s"${cols(2)}", vehicle_type = s"${cols(3)}", vehicle_brand = s"${cols(4)}", street_code = s"${cols(5)}", house_number = s"${cols(6)}", vehicle_color = s"${cols(7)}", vehicle_year = s"${cols(8)}")

        // Convert the message in Json
        val message_Json: String = Json.toJson(message).toString()
        producer.send(new ProducerRecord[String, String](topic, message_Json))

      }}




  }





  println("\nAll sent")
  producer.close()
}


