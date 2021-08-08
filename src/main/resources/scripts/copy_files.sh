#!/bin/sh
cp -R /usr/bin/spark-3.0.0-bin-hadoop3.2/working_dir/data/ /opt/workspace/
mkdir -p /opt/workspace/data/jars
cp -r /temp/jars/spark-sample/sample-spark-1.1.0.jar /opt/workspace/data/jars/

#./bin/spark-submit --deploy-mode client --properties-file ./working_dir/props_conf/Sapmle-spark.properties --class com.first.spark.SparkIngestion local:///opt/workspace/data/jars/sample-spark-1.0.0.jar file:///usr/bin/spark-3.0.0-bin-hadoop3.2/working_dir/props_conf/SampleJobConfig.conf