package com.boco.bomc.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import scala.Tuple2;

public class Kafka0_10_App {
	
	private static void testKafkaConsumer() {
		SparkConf _sparkConf = new SparkConf();
		
		JavaStreamingContext jssc = new JavaStreamingContext(_sparkConf, Durations.seconds(10));
		List<String> topics = Arrays.asList("topicA", "topicB");
		String brokers = "localhost:9092,anotherhost:9092";
		
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("metadata.broker.list", brokers) ;
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		//Topic分区
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topicA", 0), 2L); 
		
		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
                .createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics,
                                kafkaParams, offsets));
		
		 JavaPairDStream<String, String>  input = stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				return new Tuple2<>(record.key(), record.value());
			}
		});
		
		 input.print();
	}

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

		JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(jsc, props, numberOfReceivers,
				StorageLevel.MEMORY_ONLY());

		// Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
		JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
				.getPartitionOffset(unionStreams, props);

		// Start Application Logic
		unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
			@Override
			public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {
				List<MessageAndMetadata<byte[]>> rddList = rdd.collect();
				System.out.println(" Number of records in this batch " + rddList.size());
				for (MessageAndMetadata<byte[]> mmbytes : rddList) {
					// mmbytes.*;
				}
			}
		});
		// End Application Logic

		// Persists the Max Offset of given Kafka Partition to ZK
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
		if (args.length != 2) {
			System.out.println("Please input zk and brokerList:");
			System.exit(-1);
		}

		// 消费kafka消息
		sampleMonitor(args[0], args[1]);
	}

}
