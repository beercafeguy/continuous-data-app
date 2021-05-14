package com.beercafeguy.spark.streams.commons

import org.apache.spark.sql.SparkSession

object BeerCafeStreamingSessionBuilder {

  val sparkSession:SparkSession=SparkSession.builder()
    .master("local[*]")
    .appName("Spark Live Join App")
    .config("spark.sql.shuffle.partitions","4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    //properties for connecting to kafka
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
    //cassandra properties end here
    .getOrCreate()
}
