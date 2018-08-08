package com.boco.bomc.spark.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class BasicQuery {
	
	
	public void testM1() {
		String inputFile = "test.json";
		
		SparkContext sparkContext = new SparkContext();
		
		JavaSparkContext ctx = new JavaSparkContext(sparkContext);
		HiveContext sqlCtx = new HiveContext(ctx);
		
		Dataset input = sqlCtx.jsonFile(inputFile);
		// 注册输入的SchemaRDD
		input.registerTempTable("tweets");
		// 依据retweetCount（转发计数）选出推文
		Dataset<Row> topTweets = sqlCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
	}

}
