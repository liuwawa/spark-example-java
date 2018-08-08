package com.boco.bomc.spark.streaming;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class FlumeAvroApp {
	
	/**
	 * 推式接收器
	 */
	private static void testM1() {
		SparkConf _sparkConf = new SparkConf();
		
		JavaStreamingContext jssc = new JavaStreamingContext(_sparkConf, Durations.seconds(1));
		
		String receiverHostname = "localhost";
		int  receiverPort = 23001;
		
		JavaDStream<SparkFlumeEvent> events = FlumeUtils.createStream(jssc, receiverHostname,
				receiverPort);
		
		JavaDStream<AvroFlumeEvent> input = events.map(new Function<SparkFlumeEvent, AvroFlumeEvent> (){
			private static final long serialVersionUID = 1L;
			@Override
			public AvroFlumeEvent call(SparkFlumeEvent v1) throws Exception {
				return v1.event();
			}
		});
		
		
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
	
	/**
	 * 拉式接收器
	 */
	private static void testM2() {
		SparkConf _sparkConf = new SparkConf();
		
		JavaStreamingContext jssc = new JavaStreamingContext(_sparkConf, Durations.seconds(1));
		//配置检查点
		jssc.checkpoint("hdfs://bomc106:9000/spark/flume/checkpoint");
		
		String receiverHostname = "somehost";
		int  receiverPort = 23001;
		
		JavaDStream<SparkFlumeEvent> events = FlumeUtils.createPollingStream(jssc, receiverHostname,
				receiverPort);
		
		JavaDStream<String> input = events.map(new Function<SparkFlumeEvent, String> (){
			private static final long serialVersionUID = 1L;
			@Override
			public String call(SparkFlumeEvent v1) throws Exception {
				return new String(v1.event().getBody().array(), "UTF-8");
			}
		});
		
		
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
	
	
	public static void main(String[] args) {
		
		
	}

}
