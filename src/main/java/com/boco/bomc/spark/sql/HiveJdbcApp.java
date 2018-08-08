package com.boco.bomc.spark.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveJdbcApp {

    public static void main(String[] args) {
        //Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
        SparkConf conf = new SparkConf().setAppName("SOME APP NAME");
        SparkContext sc = new SparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Hive Example")
                .getOrCreate();

         Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:hive2://34.223.237.55:10000")
                .option("dbtable", "t_standard_log")
                .option("user", "hduser")
                .option("password", "hadoop")
                .option("driver", "org.apache.hive.jdbc.HiveDriver")
                .load();

        System.out.println("able to connect------------------");

        jdbcDF.show();

        jdbcDF.printSchema();

        jdbcDF.createOrReplaceTempView("std");

        Dataset<Row> sqlDF = spark.sql("select * from std");
        System.out.println("Start println-----");


        List<Row> rowList = spark.sql("select * from std").collectAsList();
        for(Row row : rowList) {
            row.get(0);
        }
        System.out.println("end println-----");
        sqlDF.show(false);
    }
}
