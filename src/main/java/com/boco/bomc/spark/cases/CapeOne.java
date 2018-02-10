package com.boco.bomc.spark.cases;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class CapeOne {

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Please input filePath");
            System.exit(-1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(args[0])
                .javaRDD()
                .map(line -> {
                    if(line != null) {
                        String[] parts = line.split("\\s+");
                        if(parts.length == 3){
                            return new Person(parts[0],parts[1],Integer.parseInt(parts[2]));
                        } else {
                            return new Person("0","C",-1);
                        }
                    } else {
                        return new Person("0","C",-1);
                    }
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        /**

        System.out.println("<display #1>");
        Dataset<Row> df180 = spark.sql("select id,gender,height from people where height > 180 and gender='M'");
        System.out.println("Men whose height are more than 180: " + df180.count());

        System.out.println("<display #2>");
        Dataset<Row> df170 = spark.sql("select id,gender,height from people where height > 170 and gender='F'");
        System.out.println("Women whose height are more than 170: " + df170.count());


        System.out.println("<display #3>");
        peopleDF.groupBy("gender").count().show();
         */


        System.out.println("<display #4>");
        System.out.println("Sorted the people by height in descend order,Show top 50 people");
        Row[] topRows = (Row[]) peopleDF.sort(peopleDF.col("height").desc()).take(50);
        for (Row row : topRows) {
            System.out.println(row.toString());
            System.out.println(row.get(0) + "," + row.get(1) + "," + row.get(2));
        }

        System.out.println("<display #6>");
        System.out.println("The Average height for Men");
        Map<String, String> aggMap = new HashMap<>();
        aggMap.put("height", "avg");
        peopleDF.filter(peopleDF.col("gender").equalTo("M")).agg(aggMap).show();

        System.out.println("<display #7>");
        System.out.println("The Max height for Women:");
        aggMap = new HashMap<>();
        aggMap.put("height", "max");
        peopleDF.filter(peopleDF.col("gender").equalTo("M")).agg(aggMap).show();

        spark.close();
        System.out.println("sparkSession closed.");

    }

}
