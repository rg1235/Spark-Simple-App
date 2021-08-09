# Large file processor

The large file processor application is developed as a dockerized application
using Spark framework (Spark 3.0) for data processing and scala for application development.

The dockerized application runs on [spark-standalone-cluster-on-docker](https://github.com/cluster-apps-on-docker/spark-standalone-cluster-on-docker) containing a spark-master and two worker nodes. It also provides a simulated hdfs.

## Steps to Run the code
**Important Note** :- The spark-master and workers have a shared workspace ```/opt/workspace``` which is the simulated HDFS.
All the directories Input/Output,application jar needs to be placed in the shared workspace so that it is accessible to all three containers.

Please follow the following steps to run the code.
1. Build the application using ```mvn clean install```.
2. cd to the projects working directory and run ```docker-compose up -d```.This command will spin up one spark-master containers and two worker containers.
3. Once the containers are up exec into the ```spark-master``` using command ```docker exec -it spark-master bash``` .
4. Now run the shell script ```/usr/bin/spark-3.0.0-bin-hadoop3.2/working_dir/scripts``` using command ```sh /usr/bin/spark-3.0.0-bin-hadoop3.2/working_dir/scripts/copy_files.sh``` . This step is performed to copy the files (input,configuration,spark-properties) to the correct location.
5. cd to the spark-binaries location i.e. ```/usr/bin/spark-3.0.0-bin-hadoop3.2```.
6. The job config and spark-properties are mounted in spark master. These files can be referred from this location in the repo ```src/main/resources/props_conf```.
7. Next step will be run the spark-job in client mode using the following command ```./bin/spark-submit --deploy-mode client --properties-file ./working_dir/props_conf/Sapmle-spark.properties --class com.first.spark.BatchProcessingMain local:///opt/workspace/data/jars/sample-spark-1.1.0.jar file:///usr/bin/spark-3.0.0-bin-hadoop3.2/working_dir/props_conf/SampleJobConfig.conf```.
8. The output present at the output filePath (As provided in the config) can be checked through spark-shell present at ```/usr/bin/spark-3.0.0-bin-hadoop3.2/bin/spark-shell```.Run spark-shell and use `spark.read.parquet(<output_path>)` to read the output as a table.
9. Similarly,same steps as mentioned in point 8 can be followed to check aggregation output.

**Note** - To run the application on a different input file please place the input on the shared workspace : ```/opt/workspace``` (Simulated HDFS) and 
update the config accordingly.

## Table details and Schema.
1. Ingested Output data Schema - 

```
|-- name: string (nullable = true)
|-- sku: string (nullable = true)
|-- description: string (nullable = true)
|-- batchtime: integer (nullable = true)
```
New column batchTime **(Ingestion time in epoch)** is introduced in order to manage the updates in the table.
2. Aggregation Output Schema
```
 |-- name: string (nullable = true)
 |-- num_products: long (nullable = true)
```

Both the output tables can be created/reproduced by running the application based upon the steps mentioned in the
previous section: Steps to Run the code.

## What is done from “Points to achieve”
1. The application is written in Scala using Spark Framework and follow OOPS concepts.
2. Using Spark as the choice for ingestion helps in parallel & non-blocking ingestion/processing of data from file/files.
   The data is read as a Dataframe and each executor/worker works on a set of partitions of data which are created when
   it is read as a Dataframe. The advantage of using Spark as framework of choice is that depending on the size of data
   the number of executors can be increased thereby horizontally scaling the ingestion.
3. The updates are handled by using the ingestion time which is ```batchtime``` in the output schema.
   Whenever updates are ingested, union is performed between already ingested data and the updates (new data), this is 
   followed by ranking the records of each key ("name","sku" in this case) based on ingestion time ```batchtime```,
   the latest record of each key will be picked. The below example was created by ingesting updates on already ingested
   data, the ```batchtime``` col with higher values shows updates have happened.

   |           name|                sku|         description| batchtime|
   |---------------|-------------------|--------------------|----------|
   |Tiffany Johnson|      do-many-avoid|     Born tree wind.|1628522171|
   |   Roger Huerta|citizen-some-middle|Important fight w...|1628522171|
   | Jessica Robles| whose-whose-growth|To church PM ever...|1628522171|
   |  Dustin Hughes|  activity-industry|Today side health...|1628522171|
   | Theresa Taylor|          step-onto|Choice should lea...|1628522171|
   |    Bryce Jones| lay-raise-best-end|Art community flo...|1628522171|
   |   Aaron Abbott|   expert-best-rate|Break drug deal w...|1628520445|
   |  Aaron Alvarez|  safe-its-call-age|Smile almost hosp...|1628520445|
   |   Aaron Acosta|    concern-between|Inside last aroun...|1628520445|
   | Aaron Anderson| act-certain-though|Part its tend. Se...|1628520445|
   | Aaron Anderson|until-although-very|Significant neces...|1628520445|

4. All the product details are ingested in the same output path in the form of parquet files.
   
   |           name|                sku|         description| batchtime|
   |---------------|-------------------|--------------------|----------|
   |    Bryce Jones| lay-raise-best-end|Art community flo...|1628520445|
   | Jessica Robles| whose-whose-growth|To church PM ever...|1628520445|
   |  John Robinson|   cup-return-guess|Produce successfu...|1628520445|
   |   Wendy Nelson|   campaign-site-in|It low image own ...|1628520445|
   | Theresa Taylor|          step-onto|Choice should lea...|1628520445|
   |Deborah Hunt MD|    possible-theory|Culture own diffe...|1628520445|
   |   Roger Huerta|citizen-some-middle|Important fight w...|1628520445|
   |   John Buckley|     term-important|Alone maybe educa...|1628520445|
   |  Dustin Hughes|  activity-industry|Today side health...|1628520445|
   |Tiffany Johnson|      do-many-avoid|     Born tree wind.|1628520445|

5. The aggregated output in present in the aggregation output path as mentioned in config file with "name" and 
   "num_products" as columns.

   |name             |num_products|
   |-----------------|------------|
   |Michael Smith    |247         |
   |Michael Johnson  |187         |
   |Robert Smith     |167         |
   |Christopher Smith|159         |
   |David Smith      |158         |
   |Michael Williams |157         |
   |John Smith       |157         |
   |James Smith      |152         |
   |Jennifer Smith   |151         |
   |Michael Brown    |148         |

**Note** - For point 4 and 5 the ingested data and aggregated data , the data can be visualized as a table using spark-shell
as the output path for each of them is same. Moreover, as an extension Hive (External) Tables can be created over the parquet data.
For point 3 choosing "name" and "sku" combined as key because of its low cardinality , therefore based upon the logic for
updates the data loss will be very low as compared to choosing "sku" as primary key.

## What is not done from “Points to achieve”
All the points mentioned are done. <br/>
Data can be visualized as tables using spark shell but a better way will be to create
external hive tables which is mentioned in the next section.

## What would you improve if given more days
1. Integration of Hive in the above application will be the first priority
   as we can easily create external table over the output parquet data which will
   help in performing interactive queries much more easily as compared to running
   a spark shell.
2. Rewriting only those partitions where updates are meant to be made instead of all the partitions.
