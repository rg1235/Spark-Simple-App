package com.first.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import java.util.TimeZone

trait SparkTestContext {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private val master = "local[1]"
  private val appName = "testing"

  val tempDirectory = Files.createTempDirectory("spark-test-warehouse").toFile
  tempDirectory.deleteOnExit()
  val projectRoot = Paths.get(".").toAbsolutePath.toString

  private val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .set("spark.sql.warehouse.dir", tempDirectory.toURI.toString)
    .set("spark.default.parallelism", "1")
    .set("spark.sql.shuffle.partitions", "1")

  def getOrCreateTestSparkSession: SparkSession = {
    SparkSession.builder().config(conf).getOrCreate()
  }

  implicit val ss: SparkSession = getOrCreateTestSparkSession
  ss.sparkContext.setLogLevel("ERROR")
}
