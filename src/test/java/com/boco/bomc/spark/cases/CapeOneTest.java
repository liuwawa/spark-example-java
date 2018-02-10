package com.boco.bomc.spark.cases;

public class CapeOneTest {

    public static void main(String[] args) {
        String msg = "499999864 M 252";
        String[] parts = msg.split("\\s+");
        if(parts.length == 3){
            System.out.println(parts[0]+","+parts[1]+","+parts[2]);
        }

    }
}
