package com.first.spark.models

case class ProductsTimestamped(
                                name: Option[String],
                                sku: Option[String],
                                description: Option[String],
                                batchTime: Option[Long]
                              )
