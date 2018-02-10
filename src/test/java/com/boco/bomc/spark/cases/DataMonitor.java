package com.boco.bomc.spark.cases;

import java.io.File;
import java.io.FileWriter;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class DataMonitor {

    /**
     * 模拟生成人员数据
     */
    private static void personDataFile() {
        String[] sex = {"F", "M"};
        Random hRandom = new Random();//50 -> 270
        AtomicLong sid = new AtomicLong(0);

        long len = 5 * 10000 * 10000L;

        String path = System.getProperty("user.dir");
        System.out.println("path:" + path);
        DecimalFormat format = new DecimalFormat("0.00");
        format.setRoundingMode(RoundingMode.HALF_UP);


        try {
            FileWriter writer = new FileWriter(new File(path + "/person-data-sample.txt"), true);
            String lineSeparator = "\n";
            while (sid.incrementAndGet() <= len) {
                //避免最后一条记录为NULL
                if (sid.get() >= len) {
                    lineSeparator = "";
                }
                writer.write(sid.get() + " " + sex[hRandom.nextInt(2)] + " " + (50 + hRandom.nextInt(220)) + lineSeparator);
                if (sid.get() % 1000000L == 0) {
                    Thread.sleep(1000L);
                    System.out.println("# " + sid.get() + " write done -> " + format.format((double) sid.get() / len * 100d) + "%");
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 模拟生成1000W用户数据
     */
    private static void userDataFile() {
        String[] sex = {"F", "M"};
        Random hRandom = new Random();
        String[] roles = new String[]{"ROLE001", "ROLE002", "ROLE003", "ROLE004", "ROLE005"};
        String[] regions = new String[]{"REG001", "REG002", "REG003", "REG004", "REG005"};
        int MAX_USER_AGE = 60;
        //how many records to be generated
        long MAX_RECORDS = 10000000l;

        String path = System.getProperty("user.dir");
        System.out.println("path:" + path);
        DecimalFormat format = new DecimalFormat("0.00");
        format.setRoundingMode(RoundingMode.HALF_UP);
        AtomicLong sid = new AtomicLong(0);

        try {
            FileWriter writer = new FileWriter(new File(path + "/user-data-sample.txt"), true);
            String lineSeparator = "\n";
            while (sid.incrementAndGet() <= MAX_RECORDS) {
                //避免最后一条记录为NULL
                if (sid.get() >= MAX_RECORDS) {
                    lineSeparator = "";
                }


                //generate the registering date for the user
                int year = hRandom.nextInt(16) + 2000;
                int month = hRandom.nextInt(12) + 1;
                //to avoid checking if it is a valid day for specific month
                //we always generate a day which is no more than 28
                int day = hRandom.nextInt(28) + 1;
                String registerDate = year + "-" + month + "-" + day;

                /**
                 *          用户ID 性别 年龄 注册时间      角色    地区
                 * 数据格式：1      M   23   2000-04-12  ROLE001 REG001
                 */
                writer.write(sid.get() + " " + sex[hRandom.nextInt(2)] + " " + (16 + hRandom.nextInt(44)) + " " + registerDate + " " + roles[hRandom.nextInt(5)] + " " + regions[hRandom.nextInt(5)] + lineSeparator);
                if (sid.get() % 100000L == 0) {
                    Thread.sleep(1000L);
                    System.out.println("# " + sid.get() + " write done -> " + format.format((double) sid.get() / MAX_RECORDS * 100d) + "%");
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 模拟1亿交易数据
     */
    private static void consumerDataFile() {
        Random hRandom = new Random();
        String[] products = new String[]{"P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008", "P009", "P010"};
        String[] prices = new String[]{"", "1298.00", "1209.00", "1001.00", "23.00", "12.00", "3.00", "123.00", "500.00", "1999.00", "1899.00"};

        //how many records to be generated
        long MAX_RECORDS = 100000000l;

        String path = System.getProperty("user.dir");
        System.out.println("path:" + path);
        DecimalFormat format = new DecimalFormat("0.00");
        format.setRoundingMode(RoundingMode.HALF_UP);
        AtomicLong sid = new AtomicLong(0);

        try {
            FileWriter writer = new FileWriter(new File(path + "/consumer-data-sample.txt"), true);
            String lineSeparator = "\n";
            while (sid.incrementAndGet() <= MAX_RECORDS) {
                //避免最后一条记录为NULL
                if (sid.get() >= MAX_RECORDS) {
                    lineSeparator = "";
                }

                //generate the registering date for the user
                int year = hRandom.nextInt(17) + 2000;
                int month = hRandom.nextInt(12) + 1;
                //to avoid checking if it is a valid day for specific month
                //we always generate a day which is no more than 28
                int day = hRandom.nextInt(28) + 1;
                String registerDate = year + "-" + month + "-" + day;

                /**
                 *          交易单号 交易日期      产品种类    产品价格 用户ID
                 * 数据格式：1       2000-04-12   P001       ￥123   123
                 */
                int pIdx = hRandom.nextInt(10);
                writer.write(sid.get() + " " + registerDate + " " + products[pIdx] + " " + prices[pIdx] + " " + hRandom.nextInt(100000000) + lineSeparator);
                if (sid.get() % 1000000L == 0) {
                    Thread.sleep(1000L);
                    System.out.println("# " + sid.get() + " write done -> " + format.format((double) sid.get() / MAX_RECORDS * 100d) + "%");
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public static void main(String[] args) {
        // 生成人员文件数据量为5亿
        //personDataFile();

        // 生成用户数据1000W
//        userDataFile();

        // 生成用户交易数据1亿
        consumerDataFile();

    }
}
