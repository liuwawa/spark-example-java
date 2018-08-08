package com.boco.bomc.spark.streaming;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


public class Kafka0_8_App {

	public static void main(String[] args) {

		JavaSparkContext conf = new JavaSparkContext();
		// 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Map<String, Integer> topics = new HashMap<String,Integer>();
		topics.put("app-log", 5);//topic <-> thread-num
		topics.put("web-log", 5);
		
		//文件方式
		String zkQuorum = "localhost:2181";
		String group = "web-app-consumer-group";
		//kafka-0.8.2
		JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, zkQuorum, group, topics);
		
		input.print();
		
		// 启动流计算环境StreamingContext并等待它"完成"
		jssc.start();
		// 等待作业完成
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	
	}

}
