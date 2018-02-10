package com.boco.bomc.spark.streaming;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class SimpleApp {

	public static void main(String[] args) {
		//1518148006014 -> 1518148120000 2018-02-09 11:48:40,000
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		System.out.println(sdf.format(new Timestamp(1518148010000L)) );
	}

}
