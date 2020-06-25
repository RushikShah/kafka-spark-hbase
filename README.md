# Twitter Data Pulling using Kafka - Spark Streaming and storing data into Hbase (kafka-spark-hbase)

In this project, Our aim is to load streaming data using Kafka and store it into NoSQL db like hbase, We will use Twitter as a our source for streaming data.
For this project, we have use 2 node Kafka cluster and 3 node Spark Cluster using YARN as a resource manager. In this project, we have just showcased the functionality of implementing Kafka with Spark Streaming, and haven't considered anything about performance tuning. we can cover performance tuning separetly but from this project, we can get insight into performance issues by understanding the way the code interacts with RDD partitioning in Spark and topic partitioning in Kafka.

# Versions of Spark & Kafka
	Installed spark 2.2.0 and kafka 2.11-2.1.0 on ubuntu machine and used JDK 1.8.0.
	zookeeper version is 3.4.13.
	Scala Version is 2.11.8
	
# Dependencies
	Following SBT dependencies are used in this project:
	
	libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.2" % Test

	libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.8.0-alpha2" % Test

	libraryDependencies += "org.slf4j" % "slf4j-api" % "1.8.0-alpha2"

	libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.8.0-alpha2" % Test

	libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.0"

	libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
	
	libraryDependencies += "org.apache.avro" % "avro" % "1.7.7"
	
	libraryDependencies += "com.twitter" %% "bijection-core" % "0.8.0"
	
	libraryDependencies += "com.twitter" %% "bijection-avro" % "0.7.0"
	
	libraryDependencies ++= Seq(
		"org.apache.hadoop" % "hadoop-core" % "0.20.2",
		"org.apache.hbase" % "hbase" % "1.4.8",
		"org.apache.hbase" % "hbase-server" % "1.4.8",
		"org.apache.hbase" % "hbase-client" % "1.4.8",
		"org.apache.hbase" % "hbase-common" % "1.4.8"
	)
	
	libraryDependencies ++= Seq(
		"org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
		"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
		"org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.0"
	)
	libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.9.2"
	libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.9.2"
	libraryDependencies += "org.apache.hadoop" % "hadoop-mapred" % "0.22.0"


