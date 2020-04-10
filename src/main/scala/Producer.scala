import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, _}
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.Random
import play.api.libs.json.Json

object Producer extends App {
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)
  val topic = "parking_ticket_violation"
  implicit val messageWrites = Json.writes[Message]

  // Lists of examples of car color, model and brand
  val color = List("BROWN", "WHITE", "BLACK", "BLUE", "SILVE", "GREEN", "RED")
  val model = List("VAN", "P-U", "SUBN", "TRLR", "DELV", "SDN")
  val brand = List("AUDI", "FORD", "CHEVR", "GMC", "DODGE", "TOYOT", "SUBAR", "HYUND", "NISSA", "VOLKS")

  def message_simulator(c: Int): Unit = {

    // Retrieves randomly a color, model and brand from the list
    val car_model = model(Random.nextInt(model.length))
    val car_brand = brand(Random.nextInt(brand.length))
    val car_color = color(Random.nextInt(color.length))
    
    // Generates a random date
    val day = 1 + Random.nextInt(29)
    val month = 1 + Random.nextInt(11)
    val year = 2015 + Random.nextInt(5)
    val date = s"$day/$month/$year"

    // Generates an error
    val error = ((c % 20) + 20) % 20

    val message ={
      // If this is a simulated error, the drone send "unknown" instead of a normal violation code
      if (error==0)
        Message(Random.nextInt(1000).toString, s"NY", date, s"unknown", car_model, car_brand, Random.nextInt(2000).toString, Random.nextInt(300).toString, car_color, (1990 +  Random.nextInt(30)).toString)
      else
        Message(Random.nextInt(1000).toString, s"NY", date, Random.nextInt(100).toString, car_model, car_brand, Random.nextInt(2000).toString, Random.nextInt(300).toString, car_color, (1990 +  Random.nextInt(30)).toString)
    }

    // Convert the message in Json
    val message_Json: String = Json.toJson(message).toString()
    if (c != 0)
    {
      producer.send(new ProducerRecord[String, String](topic, "key", message_Json))
      message_simulator(c - 1)
    }
  }

  // Simulates 30 drones
  message_simulator(30)

  producer.close()
}


