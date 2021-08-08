package com.first.spark

import com.first.spark.utils.Constants.{AggOutConfig, OutputConfig, batchTimeColName}
import com.first.spark.utils.ReadWriteUtils.{readConfigFile, writeDf}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import sys.process._

object BatchProcessingMain {

  private val logger = LoggerFactory.getLogger(getClass)
  private val appNameConfig = "appName"
  private val sparkMasterUrl = "spark_master_url"


  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new RuntimeException(
        "Config file path must be present"
      )
    }
    // Read Config file
    val configFilePath = args(0)
    val config: Config = readConfigFile(configFilePath)
    logger.info("Input Config {}", config)

    implicit val spark: SparkSession = SparkSession.builder().appName(config.getString(appNameConfig))
      .master(config.getString(sparkMasterUrl))
      .getOrCreate()

    logger.info("Application ID -> " + spark.sparkContext.applicationId)

    val processedDf: DataFrame = ProductsIngestion.process(config)
    val aggDf: DataFrame = ProductsIngestion.processAgg(processedDf)
    val outputConfigTemp = ConfigFactory
      .parseString(""""output": {"format" : "parquet", "filePath" : "/opt/workspace/data/temp_output/"}""")
    val outputConfig: Config = config.getConfig(OutputConfig)
    val aggOutConfig: Config = config.getConfig(AggOutConfig)
    writeDf(processedDf, outputConfigTemp.getConfig("output") , Some(batchTimeColName))
    writeDf(aggDf, aggOutConfig)
    spark.close()
    // Copying the data from temp directory to output directory
    logger.info("Copying data from temporary output directory" +
      " to actual output directory")
    val outputPath = outputConfig.getString("filePath")
    try {
      val res = "sh /usr/bin/spark-3.0.0-bin-hadoop3.2/working_dir/scripts/copy_output.sh " + outputPath !
    }
    catch{
      case x:Exception =>
        print("Exception occurred while copying output from temp location to output location -> ",x)
    }
  }

}
