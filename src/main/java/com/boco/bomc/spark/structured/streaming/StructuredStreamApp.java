package com.boco.bomc.spark.structured.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredStreamApp {
	
	
	private static SparkSession sparkSession(){
		SparkSession spark = SparkSession
				  .builder()
				  .appName("JavaStructuredNetworkWordCount")
				  .getOrCreate();
		return spark;
	}
	
	private static void testSocketListener(){
		SparkSession spark = sparkSession();
		
		// Create DataFrame representing the stream of input lines from connection to localhost:9999
		Dataset<Row> lines = spark
		  .readStream()
		  .format("socket")
		  .option("host", "localhost")
		  .option("port", 9999)
		  .load();

		// Split the lines into words
		Dataset<String> words = lines
		  .as(Encoders.STRING())
		  .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

		// Group the data by window and word and compute the count of each group
		Dataset<Row> windowedCounts2 = words
		    .withWatermark("timestamp", "10 minutes")
		    .groupBy(
		        functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
		        words.col("word"))
		    .count();
		
		// Group the data by window and word and compute the count of each group
		Dataset<Row> windowedCounts = words.groupBy(
		  functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
		  words.col("word")
		).count();
		
		// Generate running word count
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
		// Start running the query that prints the running counts to the console
		StreamingQuery query = wordCounts.writeStream()
		  .outputMode("complete")
		  .format("console")
		  .start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
		
	}
	

	public static void main(String[] args) {

	}

}
