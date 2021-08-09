package com.first.spark

import com.first.spark.utils.Constants.{InputConfig, OutputConfig, batchTimeColName}
import com.first.spark.utils.ReadWriteUtils.{checkDirectoryExists, readDataFrame}
import com.typesafe.config.Config
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, count, lit, rank}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ProductsIngestion {

  def process(config: Config)(implicit spark: SparkSession): DataFrame = {

    val inputConfig: Config = config.getConfig(InputConfig)
    val outputConfig: Config = config.getConfig(OutputConfig)

    //read raw data from csv
    val df: DataFrame = readDataFrame(inputConfig).dropDuplicates()

    // Create a broadcast variable for epoch timestamp in seconds
    val current_time: Long = System.currentTimeMillis() / 1000
    val batchTime: Broadcast[Long] = spark.sparkContext.broadcast(current_time)

    // Check if output exists
    val checkOldInputPath: Boolean = checkDirectoryExists(outputConfig.getString("filePath"))

    checkOldInputPath match {
      case true => //update existing output
        updateExistingData(df,outputConfig, batchTime)
      case false => //append new data
        df.withColumn(batchTimeColName, lit(batchTime.value))
    }
  }

  private def updateExistingData(df: DataFrame, outputConfig: Config,
                                 batchTime: Broadcast[Long])(implicit spark: SparkSession): DataFrame = {

    val existingDf: DataFrame = readDataFrame(outputConfig)

    val dfTimeStamped: DataFrame = df.withColumn(batchTimeColName, lit(batchTime.value))
    val unionDf: Dataset[Row] = dfTimeStamped.unionByName(existingDf)

    // Creating window on name,sku as that seems better as compared to only sku
    val windowSpec: WindowSpec = Window.partitionBy("name", "sku")
      .orderBy(col(batchTimeColName).desc)
    val rankedDf: DataFrame = unionDf.withColumn("rank", rank().over(windowSpec))
    // Only selecting records with rank 1 as it will be the latest timestamp
    rankedDf.filter(col("rank") === 1).drop("rank")
  }

  // Aggregations on updated data
  def processAgg(df: DataFrame): DataFrame = {
    val aggregatedDf = df.groupBy("name")
      .agg(count(lit(1)).as("num_products"))
    aggregatedDf
  }
}
