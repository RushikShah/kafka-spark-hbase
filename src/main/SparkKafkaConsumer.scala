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
    val kafkaArr : Array[String] = Array("VM-MACRA1:9092,VM-MACRA2:9092", "Twitter_Data")
    // val brokers = Array("VM-MACRA1:9092","VM-MACRA2:9092","VM-HPS7:9092")
    // val topics =  "SparkStreamLogs"
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

    ssc.checkpoint("hdfs://VM-MACRA1:9000/home/spark_data_checkpoint")

    var offsetRanges = Array.empty[OffsetRange]
    /*
    val windoedStream1 = messages.window(Minutes(1))
    windoedStream1.foreachRDD(rdd => {
    rdd.foreach(println)
       if (!rdd.isEmpty()) {
         rdd.saveAsTextFile("/home/spark_data")
       }
    }) */
    
    /*
    messages.foreachRDD ( rdd => {
      println(rdd)
      val conf = HBaseConfiguration.create()
          conf.set(TableOutputFormat.OUTPUT_TABLE, "Kafka_Stream")
          conf.set("hbase.zookeeper.quorum", "VM-MACRA1:2181")
          conf.set("hbase.master", "VM-MACRA1:60010")
          conf.set("hbase.rootdir", "hdfs://VM-MACRA1:9000/user/nituser/hbase")

      val jobConf: jobConfig  = new JobConf(conf, this.getClass)
          jobConf.setOutputFormat(classOf[**TableOutputFormat**])
          jobConf.set(**TableOutputFormat**.OUTPUT_TABLE, "Kafka_Stream")

         
      rdd.saveAsHadoopDataset(jobConf)
    })
    */
    var i = 1

    /*
    val hRConf = HBaseConfiguration.create()
        hRConf.set(TableOutputFormat.OUTPUT_TABLE, "Kafka_Stream")
        hRConf.set("hbase.zookeeper.quorum", "VM-MACRA1:2181")
        hRConf.set("hbase.master", "VM-MACRA1:60010")
        hRConf.set("hbase.rootdir", "hdfs://VM-MACRA1:9000/user/nituser/hbase")
        hRConf.set(TableInputFormat.INPUT_TABLE, "Kafka_Stream")
   
    val hBaseRDD = sc.newAPIHadoopRDD(hRConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    i = hBaseRDD.count().toInt
    */
    val r = scala.util.Random

    messages.foreachRDD ( rdd => {
      rdd.foreachPartition(iter => {
        val hConf = HBaseConfiguration.create()
            hConf.set(TableOutputFormat.OUTPUT_TABLE, "Kafka_Stream")
            hConf.set("hbase.zookeeper.quorum", "VM-MACRA1:2181")
            hConf.set("hbase.master", "VM-MACRA1:60010")
            hConf.set("hbase.rootdir", "hdfs://VM-MACRA1:9000/user/nituser/hbase")

        val hTable = new HTable(hConf, "Kafka_Stream")

        iter.foreach(record => {
          i += 1
          println("Increment Row Value" + i)
          val thePut = new Put(Bytes.toBytes(r.nextInt)) 
          thePut.add(Bytes.toBytes("Details"), Bytes.toBytes("sparkData"), Bytes.toBytes(record._2.toString)) 

          //missing part in your code
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
