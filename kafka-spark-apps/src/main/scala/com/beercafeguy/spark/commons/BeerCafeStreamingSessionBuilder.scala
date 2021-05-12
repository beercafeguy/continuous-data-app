package com.beercafeguy.spark.commons

import org.apache.spark.sql.SparkSession

object BeerCafeStreamingSessionBuilder {

  val sparkSession:SparkSession=SparkSession.builder()
    .master("local[*]")
    .appName("Kafka Invoice App")
    .config("spark.sql.shuffle.partitions","4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
}
