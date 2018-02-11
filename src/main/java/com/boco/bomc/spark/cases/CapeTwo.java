package com.boco.bomc.spark.cases;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class CapeTwo {

    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Please input filePaths: user-data-sample.txt consumber-data-sample.txt");
            System.exit(-1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkSQL")
                .getOrCreate();

        // Create an RDD of User objects from a text file
        JavaRDD<User> userRDD = spark.read()
                .textFile(args[0])
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\s+");
                    return new User(parts[0],parts[1],Integer.parseInt(parts[2]),parts[3],parts[4],parts[5]);
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> userDF = spark.createDataFrame(userRDD, User.class);
        // Register the DataFrame as a temporary view
        userDF.createOrReplaceTempView("user");

        JavaRDD<ConsumerRcd> orderRDD = spark.read()
                .textFile(args[1])
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\s+");
                    return new ConsumerRcd(parts[0],parts[1],parts[2],parts[3],parts[4]);
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> orderDF = spark.createDataFrame(userRDD, ConsumerRcd.class);
        // Register the DataFrame as a temporary view
        orderDF.createOrReplaceTempView("order");


        userDF.persist(StorageLevel.MEMORY_AND_DISK_SER());
        orderDF.persist(StorageLevel.MEMORY_AND_DISK_SER());

        long count = orderDF.filter(orderDF.col("dealDate").contains("2015")).join(
                userDF, orderDF.col("uid").equalTo(userDF.col("uid"))).count();
        System.out.println("The number of people who have orders in the year 2015:" + count);

        count = spark.sql("SELECT * FROM order where dealDate like '2014%'").count();
        System.out.println("total orders produced in the year 2014:" + count);

        //Orders that are produced by user with ID 1 information overview
        spark.sql("SELECT o.did,o.prodId,o.prodPrice,u.uid FROM order o,user u where u.uid = 1 and u.uid = o.uid").show();
        System.out.println("Orders produced by user with ID 1 showed.");

        //Calculate the max,min,avg prices for the orders that are producted by user with ID 10
        Dataset<Row> dsRow = spark.sql("SELECT max(o.prodPrice) as maxPrice,min(o.prodPrice) as minPrice,avg(o.prodPrice) as avgPrice,u.uid FROM order o,user u where u.uid = 10 and u.uid = o.uid group by u.uid");
        System.out.println("Order statistic result for user with ID 10:");
        Row[] rows = (Row[]) dsRow.collect();
        for(Row row : rows) {
            System.out.println("Minimum Price=" + row.getAs("minPrice") + ";Maximum Price=" + row.getAs("maxPrice") + ";Average Price=" + row.getAs("avgPrice") );
        }

        spark.close();
        System.out.println("sparkSession closed.");

    }

}
