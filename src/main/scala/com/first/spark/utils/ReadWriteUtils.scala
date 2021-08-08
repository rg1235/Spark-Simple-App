package com.first.spark.utils

import com.first.spark.models.Products
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Encoders, Row, SaveMode, SparkSession}

import java.io.{File, IOException}

object ReadWriteUtils {

  def checkDirectoryExists(path : String) : Boolean = {
    val dirPath = new File(path)
    dirPath.exists()
  }

  // Support for csv,parquet,orc
  def readDataFrame(config: Config)(implicit spark: SparkSession) : DataFrame = {
    val format = config.getString("format")
    val df : DataFrame = {
      if (format == "csv") {
        spark.read.schema(Encoders.product[Products].schema)
          .option("header", config.getBoolean("header"))
          .csv(config.getString("filePath"))
      }
      else if (format == "orc") {
        spark.read.orc(config.getString("filePath"))
      }
      else if (format == "parquet") {
        spark.read.parquet(config.getString("filePath"))
      }
      else {
        throw new RuntimeException("File format not supported")
      }
    }
    df
  }

  // Write df as parquet
  def writeDf(df: DataFrame, config: Config, partitionCol: Option[String] = None) : Unit = {

    val partitionedDf: DataFrameWriter[Row] = partitionCol match {
      case Some(colName) => df.write.partitionBy(colName)
      case None => df.write
    }

    val format: String = config.getString("format")
    if(format == "parquet"){
      partitionedDf.mode(SaveMode.Overwrite).parquet(config.getString("filePath"))
    }
  }

  def readConfigFile(filePath : String) : Config = {
    val conf: Configuration = new Configuration
    val fsPath: org.apache.hadoop.fs.Path = new org.apache.hadoop.fs.Path(filePath)
    val fs: FileSystem = FileSystem.get(fsPath.toUri, conf)
    var inputStream: FSDataInputStream = null
    try {
      if (!fs.exists(fsPath)) {
        throw new IOException("Input file not found")
      }
      inputStream = fs.open(fsPath)
      ConfigFactory.parseString(IOUtils.toString(fs.open(fsPath), "UTF-8"))
    } catch {
      case e : IOException =>
        throw e
    } finally {
      inputStream.close()
      fs.close()
    }
  }

}
