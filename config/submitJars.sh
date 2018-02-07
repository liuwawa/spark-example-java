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

########################################################################################################################################
# ****** 启动应用 ******
#--master参数 spark://host:port, mesos://host:port, yarn, or local
#./spark-submit --master spark://172.18.254.106:7077[spark://172.18.254.106:6066] \
#               --deploy-mode cluster[client] \
#			   --class com.boco.bomc.spark.App \
#			   --name JavaDirectKafkaWordCount \
#			   hdfs://172.18.254.106:9000/spark/jars/spark-example-0.0.1.jar[file:///opt/BOCO/spark-2.2.1/bin/spark-example-0.0.1.jar] \
#			   172.18.254.105:29092,172.18.254.106:29092,172.18.254.107:29092 spark-topic
########################################################################################################################################

############################################################################################
# ****** 停止启动的应用 ******
# 方式一
# 1. bin/spark-submit --kill driver-20180206181114-0011 --master spark://172.18.254.106:6066
# 方式二
# 2. yarn application -kill[status|list] application_1517193589029_0001
############################################################################################

./spark-submit --master spark://172.18.254.106:7077 \
               --deploy-mode cluster \
			   --class com.boco.bomc.spark.App \
			   --name JavaDirectKafkaWordCount \
			   hdfs://172.18.254.106:9000/spark/jars/spark-example-0.0.1.jar \
			   172.18.254.105:29092,172.18.254.106:29092,172.18.254.107:29092 spark-topic