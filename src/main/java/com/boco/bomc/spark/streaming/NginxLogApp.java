package com.boco.bomc.spark.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 分析Nginx日志，统计各种PV
 * @author Administrator
 *
 */
public class NginxLogApp {

	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setAppName("NginxAnay");

		// 设计计算的周期，单位秒
		long batch = 10L;

		/*
		 * 这是bin/spark-shell交互式模式下创建StreamingContext的方法 非交互式请使用下面的方法来创建
		 */
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(batch));
		/*
		 * 创建输入DStream，是文本文件目录类型 本地模式下也可以使用本地文件系统的目录，比如 file:///home/spark/streaming
		 */
		JavaDStream<String> lines = ssc.textFileStream("hdfs:///spark/streaming");
		/*
		 * 下面是统计各项指标，调试时可以只进行部分统计，方便观察结果
		 */

		// 1. 总PV
		lines.count().print();

		// 2. 各IP的PV，按PV倒序
		// 空格分隔的第一个字段就是IP
		lines.mapToPair(new PairFunction<String, String, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				return new Tuple2<String, Long>(t.split(" ")[0], new Long(1L));
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).transformToPair(new Function<JavaPairRDD<String, Long>, JavaPairRDD<String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public JavaPairRDD<String, Long> call(JavaPairRDD<String, Long> v1) throws Exception {
				return v1.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<String, Long> t) throws Exception {
						return new Tuple2<Long, String>(t._2, t._1);
					}

				}).sortByKey(false).mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(Tuple2<Long, String> t) throws Exception {
						return new Tuple2<String, Long>(t._2, t._1);
					}
				});
			}
		}).print();

		// 3. 搜索引擎PV
		JavaDStream<String> refer = lines.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				return v1.split("\"")[3];
			}
		});
		// 先输出搜索引擎和查询关键词，避免统计搜索关键词时重复计算
		// 输出(host, query_keys)
		JavaPairDStream<String, String> searchEnginInfo = refer.mapToPair(new PairFunction<String,String,String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String v1) throws Exception {
				String[] f = v1.split("\\/");

				Map<String, String> searchEngines = new HashMap<String, String>();
				searchEngines.put("www.google.cn", "q");
				searchEngines.put("www.yahoo.com", "p");
				searchEngines.put("cn.bing.com", "q");
				searchEngines.put("www.baidu.com", "wd");
				searchEngines.put("www.sogou.com", "query");

				Tuple2<String, String> tuple2 = new Tuple2<String, String>("", "");
				if (f.length > 2) {
					String host = f[2];
					if (searchEngines.containsKey(host)) {
						String query = v1.split("\\?")[1];
						if (query.length() > 0) {
							String[] arr_search_q = query.split("\\&");
							List<String> qList = new ArrayList<String>();
							for (String search_q : arr_search_q) {
								if (search_q.indexOf(searchEngines.get(host) + "=") == 0) {
									qList.add(search_q);
								}
							}

							if (qList.size() > 0) {
								tuple2 = new Tuple2<String, String>(host, qList.get(0).split("\\=")[1]);
							} else {
								tuple2 = new Tuple2<String, String>(host, "");
							}

						} else {
							tuple2 = new Tuple2<String, String>(host, "");
						}
					}
				}
				return tuple2;
			}
		});
		
		// 输出搜索引擎PV
		searchEnginInfo.filter(new Function<Tuple2<String,String>, Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return v1._1.length() > 0;
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String,Long>(t._1, new Long(1L));
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).print();
		
		// 4. 关键词PV
		searchEnginInfo.filter(new Function<Tuple2<String,String>, Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return v1._2.length() > 0;
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String,Long>(t._2, new Long(1L));
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).print();
		
		// 5. 终端类型PV
		lines.map(new Function<String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				return v1.split("\"")[5];
			}
		}).mapToPair(new PairFunction<String,String,Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				String[] types = new String[] {"iPhone", "Android"};
				String r = "Default";
				for(String type : types) {
					if(t.indexOf(type) != -1) {
						r = type;
						break;
					}
				}
				return new Tuple2<String,Long>(r, new Long(1));
			}
		}).reduceByKey(new Function2<Long,Long,Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).print();
		
		// 6. 各页面PV
		lines.mapToPair(new PairFunction<String, String, Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				return new Tuple2<String,Long>(t.split("\"")[1].split(" ")[1], new Long(1));
			}
		}).reduceByKey(new Function2<Long,Long,Long>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).print();
		
		if(ssc != null){
			ssc.start();
		}
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
