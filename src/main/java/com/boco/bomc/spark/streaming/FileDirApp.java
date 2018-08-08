package com.boco.bomc.spark.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FileDirApp {

	public static void main(String[] args) {

		JavaSparkContext conf = new JavaSparkContext();
		// 从SparkConf创建StreamingContext并指定10秒钟的批处理大小
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		//文件方式
		String logsDirectory = "/flume/busilogs";
		JavaDStream<String> logData = jssc.textFileStream(logsDirectory);

		// 从DStream中筛选出包含字符串"error"的行
		JavaDStream<String> errorLines = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String line) {
				return line.contains("error");
			}
			
		});
		// 打印出有"error"的行
		errorLines.print();
		
		
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
