package com.beercafeguy.spark.commons

import org.apache.spark.sql.SparkSession

object SessionBuilder {

  val streamingSession = SparkSession.builder()
    .appName("Streaming WC")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.shuffle.partitions",4)
    .master("local[*]")
    .getOrCreate()
}
