package com.boco.bomc.spark.structured.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class KafkaApp {
	
	private static SparkSession sparkSession(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("JavaStructuredNetworkWordCount")
				  .getOrCreate();
		return spark;
	}
	
	private static void streamQuery(){
		SparkSession spark = sparkSession();
		// Subscribe to 1 topic
		Dataset<Row> df = spark
		  .readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1")
		  .load();
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		// Subscribe to multiple topics
		Dataset<Row> df1 = spark
		  .readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1,topic2")
		  .load();
		df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		// Subscribe to a pattern
		Dataset<Row> df2 = spark
		  .readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribePattern", "topic.*")
		  .load();
		df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
	}
	
	private static void batchQuery(){
		SparkSession spark = sparkSession();
		
		// Subscribe to 1 topic defaults to the earliest and latest offsets
		Dataset<Row> df = spark
		  .read()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1")
		  .load();
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		// Subscribe to multiple topics, specifying explicit Kafka offsets
		Dataset<Row> df1 = spark
		  .read()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1,topic2")
		  .option("startingOffsets", "{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
		  .option("endingOffsets", "{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")
		  .load();
		df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		// Subscribe to a pattern, at the earliest and latest offsets
		Dataset<Row> df2 = spark
		  .read()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribePattern", "topic.*")
		  .option("startingOffsets", "earliest")
		  .option("endingOffsets", "latest")
		  .load();
		df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
	}
	
	private static void writeStream(){
		SparkSession spark = sparkSession();
		
		// Subscribe to 1 topic
		Dataset<Row> df = spark
		  .readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1")
		  .load();
		
		// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
		StreamingQuery ds = df
		  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		  .writeStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("topic", "topic1")
		  .start();

		// Write key-value data from a DataFrame to Kafka using a topic specified in the data
		StreamingQuery ds2 = df
		  .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
		  .writeStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .start();
	}
	
	private static void writeBatches(){
		SparkSession spark = sparkSession();
		
		// Subscribe to 1 topic defaults to the earliest and latest offsets
		Dataset<Row> df = spark
		  .read()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("subscribe", "topic1")
		  .load();
		
		// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		  .write()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .option("topic", "topic1")
		  .save();

		// Write key-value data from a DataFrame to Kafka using a topic specified in the data
		df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
		  .write()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		  .save();
		
	}
	

	public static void main(String[] args) {

	}

}
