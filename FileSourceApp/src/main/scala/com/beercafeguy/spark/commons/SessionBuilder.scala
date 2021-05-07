package com.beercafeguy.spark.commons

import org.apache.spark.sql.SparkSession

object SessionBuilder {

  val streamingSession = SparkSession.builder()
    .appName("File Streaming Demo")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.shuffle.partitions",4)
    .config("spark.sql.streaming.schemaInference",true)
    .master("local[*]")
    .getOrCreate()
}
