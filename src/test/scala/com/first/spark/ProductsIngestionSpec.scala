package com.first.spark

import com.first.spark.utils.ReadWriteUtils.readConfigFile
import org.apache.spark.sql.SaveMode
import org.scalatest.FunSuite

class ProductsIngestionSpec extends FunSuite with SparkTestContext{

  test("should return raw data as output when no output exists") {

    val config = readConfigFile("src/test/resources/Config1.conf")

    val output = ProductsIngestion.process(config)

    val expected = ss.read.option("header",true).csv("src/test/resources/products.csv")

    assertResult(expected.collect())(output.drop("batchTime").collect())
    }

  test("should update existing output") {

    import ss.implicits._

    val config = readConfigFile("src/test/resources/Config2.conf")

    val existingData = Seq(("name1","sku1","this is the old record",1628367469),
      ("name5","sku4","this is the old record",1628367469),
      ("name6","sku6","this is a new prod",1628367469)).toDF("name","sku","description","batchTime")

   existingData.write.mode(SaveMode.Overwrite).parquet("src/test/resources/output2")


    val output = ProductsIngestion.process(config)

   // output.write.mode(SaveMode.Overwrite).parquet("src/test/resources/output2")

    val expected = Seq(("name1","sku1","this is a new prod"),
      ("name2","sku2","this is a new prod"),
      ("name3","sku3","this is a new prod"),
      ("name4","sku4","this is a new prod"),
      ("name5","sku4","this is a new prod"),
      ("name6","sku6","this is a new prod")).toDF("name","sku","description")

    assertResult(expected.collect())(output.drop("batchTime").collect())
  }

  test("should aggregate the products data") {

    import ss.implicits._

    val existingData = Seq(("name1","sku1","prod1",1628367469),
      ("name1","sku2","prod2",1628367469),
      ("name3","sku3","prod1",1628367469),
      ("name3","sku4","prod2",1628367470),
      ("name3","sku5","prod3",1628367470),
      ("name5","sku4","this is a new prod",1628367469),
      ("name6","sku6","this is a new prod",1628367470)).toDF("name","sku","description","batchTime")


    val output = ProductsIngestion.processAgg(existingData)

    val expected = Seq(("name1",2),
      ("name3",3),
      ("name5",1),
      ("name6",1)).toDF("name","num_products")

    assertResult(expected.collect())(output.collect())
  }

  }


