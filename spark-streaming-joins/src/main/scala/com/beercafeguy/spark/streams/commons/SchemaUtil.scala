package com.beercafeguy.spark.streams.commons

import org.apache.spark.sql.types._

object SchemaUtil {


  val loginSchema=StructType(
    List(
      StructField("login_time",StringType),
      StructField("login_id",StringType)
    )
  )
}
