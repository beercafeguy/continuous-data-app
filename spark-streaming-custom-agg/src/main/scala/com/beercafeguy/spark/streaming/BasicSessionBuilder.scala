package com.beercafeguy.spark.streaming

import org.apache.spark.sql.SparkSession

object BasicSessionBuilder {

  val spark=SparkSession.builder()
    .appName("Custom Agg")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4")
    .getOrCreate()
}
