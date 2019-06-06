import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, Properties}
import scala.io.Source
import java.io
import java.io.File
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import TwitterStream.OnTweetPosted
import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import twitter4j.{Status, FilterQuery}

object KafkaScalaProducer {
    val topic = "Twitter_Data";

    val kafkaProducer = {
      val brokers = "Server1:9092,Server2:9092";
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("client.id", "Producer")
      props.put("request.required.acks", "1")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
   }

   def main(args: Array[String]) = {
      println("Hello KafkaProducer!")
         
      val twitterStream = TwitterStream.getStream
      twitterStream.addListener(new OnTweetPosted(s => sendToKafka(s)))
      
      twitterStream.filter(new FilterQuery().track("#TwitterHashTag")) 
      //producer.close()
   }

  private def sendToKafka(s:Status) {
      //val tweetEnc = toBinary[Tweet].apply(t)
      val msg = new ProducerRecord[String, String](topic,"Created At: " + s.getCreatedAt + "@" + "Current User RetweetId: " + s.getCurrentUserRetweetId + "@" + "FavoriteCount: " + s.getFavoriteCount + "@" + "Get Location: " + s.getGeoLocation + "@" + "Retweet Count: " + s.getRetweetCount + "@" + "Get Place: " + s.getPlace + "@" + "Get Lang: " + s.getLang + "@" + "Get Text: " + s.getText + "@" + "Get User: " + s.getUser + "@" + "Get Retweet: " + s.isRetweet + "@" + "Get Retweeted: " + s.isRetweeted)
      println("Tweet Status: " + " IsRetweet:" + s.isRetweet + " GeoLocation:" + s.getGeoLocation)
      kafkaProducer.send(msg)
  }
}
