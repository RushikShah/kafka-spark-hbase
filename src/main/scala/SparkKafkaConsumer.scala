import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.spark._

import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.{ TableInputFormat, TableOutputFormat }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, Put, HTable, Result }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object DirectKafkaSparkStream {
  def main(args: Array[String]) {
    val kafkaArr : Array[String] = Array("Server1:9092,Server2:9092", "Twitter_Data")
    val Array(brokers, topics) = kafkaArr

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaSparkStream")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    ssc.checkpoint("hdfs://Server1:9000/home/spark_data_checkpoint")

    var offsetRanges = Array.empty[OffsetRange]
    
    var i = 1

    val r = scala.util.Random

    messages.foreachRDD ( rdd => {
      rdd.foreachPartition(iter => {
        val hConf = HBaseConfiguration.create()
            hConf.set(TableOutputFormat.OUTPUT_TABLE, "Kafka_Stream")
            hConf.set("hbase.zookeeper.quorum", "ZooKeeperServer1:2181")
            hConf.set("hbase.master", "HbaseMaster1:60010")
            hConf.set("hbase.rootdir", "hdfs://HbaseMaster1:9000/user/nituser/hbase")

        val hTable = new HTable(hConf, "Kafka_Stream")

        iter.foreach(record => {
          i += 1
          println("Increment Row Value" + i)
          val thePut = new Put(Bytes.toBytes(r.nextInt)) 
          thePut.add(Bytes.toBytes("Details"), Bytes.toBytes("sparkData"), Bytes.toBytes(record._2.toString)) 

          hTable.put(thePut);
        })

      })
      i += rdd.count().toInt
    })
     
    //val windoedStream1 = messages.window(Minutes(1)) 
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
