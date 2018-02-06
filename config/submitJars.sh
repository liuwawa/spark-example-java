#!/bin/sh

#
#./bin/spark-submit \
#  --class <main-class> \
#  --master <master-url> \
#  --deploy-mode <deploy-mode> \
#  --conf <key>=<value> \
#  ... # other options
#  <application-jar> \
#  [application-arguments]
#


#./spark-submit --master spark://172.18.254.106:7077[spark://172.18.254.106:6066] \
#               --deploy-mode cluster[client] \
#			   --class com.boco.bomc.spark.App \
#			   --name JavaDirectKafkaWordCount \
#			   hdfs://172.18.254.106:9000/spark/jars/spark-example-0.0.1.jar[file:///opt/BOCO/spark-2.2.1/bin/spark-example-0.0.1.jar] \
#			   172.18.254.105:29092,172.18.254.106:29092,172.18.254.107:29092 spark-topic

./spark-submit --master spark://172.18.254.106:7077 \
               --deploy-mode cluster \
			   --class com.boco.bomc.spark.App \
			   --name JavaDirectKafkaWordCount \
			   hdfs://172.18.254.106:9000/spark/jars/spark-example-0.0.1.jar \
			   172.18.254.105:29092,172.18.254.106:29092,172.18.254.107:29092 spark-topic