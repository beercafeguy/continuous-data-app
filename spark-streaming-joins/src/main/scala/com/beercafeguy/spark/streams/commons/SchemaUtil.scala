package com.beercafeguy.spark.streams.commons

import org.apache.spark.sql.types._

object SchemaUtil {


  val loginSchema=StructType(
    List(
      StructField("login_time",StringType),
      StructField("login_id",StringType)
    )
  )

  val impressionSchema = StructType(List(
    StructField("ImpressionId", StringType),
    StructField("CreatedTime", StringType),
    StructField("Campaigner", StringType)
  ))

  val clickSchema = StructType(List(
    StructField("ImpressionId", StringType),
    StructField("CreatedTime", StringType)
  ))
}
