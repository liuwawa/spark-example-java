package com.boco.bomc.spark.kafka;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.List;
import java.util.Iterator;
import java.util.Properties;

public class KafkaSparkConsumerApp {

    private static void simpleTest() {
        Properties props = new Properties();
        props.put("zookeeper.hosts", "x.x.x.x");
        props.put("zookeeper.port", "2181");
        props.put("kafka.topic", "mytopic");
        props.put("kafka.consumer.id", "kafka-consumer");
        props.put("bootstrap.servers", "x.x.x.x:9092");
// Optional Properties
        props.put("max.poll.records", "250");
        props.put("consumer.fillfreqms", "1000");

        SparkConf _sparkConf = new SparkConf();
        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(30));
// Specify number of Receivers you need.
        int numberOfReceivers = 3;

        JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(
                jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

//Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
        JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
                .getPartitionOffset(unionStreams, props);

//Start Application Logic
        unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
            @Override
            public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {
                List<MessageAndMetadata<byte[]>> rddList = rdd.collect();
                System.out.println(" Number of records in this batch " + rddList.size());
                for (MessageAndMetadata<byte[]> mmbytes : rddList) {
                    //mmbytes.*;
                }
            }
        });
//End Application Logic

//Persists the Max Offset of given Kafka Partition to ZK
        ProcessedOffsetManager.persists(partitonOffset, props);

        try {
            jsc.start();
            jsc.awaitTermination();
        } catch (Exception ex) {
            jsc.ssc().sc().cancelAllJobs();
            jsc.stop(true, false);
            System.exit(-1);
        }
    }

    private static void sampleMonitor(String zk, String brokerList) {
        Properties props = new Properties();
        props.put("zookeeper.hosts", zk);
        props.put("zookeeper.port", "2181");
        props.put("kafka.topic", "mytopic");
        props.put("kafka.consumer.id", "kafka-consumer");
        // Optional Properties
        // Optional Properties
        props.put("consumer.forcefromstart", "true");
        props.put("max.poll.records", "100");
        props.put("consumer.fillfreqms", "1000");
        props.put("consumer.backpressure.enabled", "true");
        //Kafka properties
        props.put("bootstrap.servers", brokerList);
//        props.put("security.protocol", "SSL");
//        props.put("ssl.truststore.location","~/kafka-securitykafka.server.truststore.jks");
//        props.put("ssl.truststore.password", "test1234");

        SparkConf _sparkConf = new SparkConf();
        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(30));
        // Specify number of Receivers you need.
        int numberOfReceivers = 1;

        JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(
                jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

        //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
        JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
                .getPartitionOffset(unionStreams, props);


        //Start Application Logic
        unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
            @Override
            public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<MessageAndMetadata<byte[]>>>() {

                    @Override
                    public void call(Iterator<MessageAndMetadata<byte[]>> mmItr) throws Exception {
                        while (mmItr.hasNext()) {
                            MessageAndMetadata<byte[]> mm = mmItr.next();
                            byte[] key = mm.getKey();
                            byte[] value = mm.getPayload();
                            System.out.println("Key :" + new String(key) + " Value :" + new String(value));

//                            Headers headers = mm.getHeaders();
//                            if(headers != null) {
//                                Header[] harry = headers.toArray();
//                                for(Header header : harry) {
//                                    String hkey = header.key();
//                                    byte[] hvalue = header.value();
//                                    System.out.println("Header Key :" + hkey + " Header Value :" + new String(hvalue));
//                                }
//                            }
                        }

                    }
                });
            }
        });
        //End Application Logic

        //Persists the Max Offset of given Kafka Partition to ZK
        ProcessedOffsetManager.persists(partitonOffset, props);

        try {
            jsc.start();
            jsc.awaitTermination();
        } catch (Exception ex) {
            jsc.ssc().sc().cancelAllJobs();
            jsc.stop(true, false);
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Please input zk and brokerList:");
            System.exit(-1);
        }

        //消费kafka消息
        sampleMonitor(args[0], args[1]);
    }

}
