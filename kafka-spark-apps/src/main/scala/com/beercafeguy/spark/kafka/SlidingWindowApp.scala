package com.beercafeguy.spark.kafka

import com.beercafeguy.spark.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SlidingWindowApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession
    import spark.implicits._

    val invoiceStream=spark
      .readStream //change to read to make it a simple batch app
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "sensor_topic")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()

    val valueDF = invoiceStream.select(
      col("key").cast("string").alias("SensorID"),
      from_json(col("value").cast("string"), SchemaUtil.sensorSchema).alias("value"))
      .select("SensorID","value.*")

    //valueDF.printSchema()
    val mappedDF=valueDF
      .withColumn("CreatedTime",to_timestamp($"CreatedTime","yyyy-MM-dd HH:mm:ss"))
    //mappedDF.printSchema()

    val windowedDF=mappedDF
      .withWatermark("CreatedTime","30 minute")
      .groupBy(col("SensorID"),window($"CreatedTime","15 minute","5 minute"))
    val resultDF=windowedDF
      .agg(max($"Reading").as("Max_Temp"))
      .select($"SensorID",$"window.start",$"window.end",$"Max_Temp")
    resultDF.printSchema()

    val windowedQuery=resultDF.writeStream
      .format("console")
      .outputMode("update")
      // in complete mode the whole state will be sent to output -> no cleanup of state store due to water mark
      // so watermark will be overridden by complete mode
      // In Append -> not allowed in aggregates without watermark (will only emit record to sink if it won't update in future)
      .option("checkpointLocation", "chk-point-dir/slide_window")
      .trigger(Trigger.ProcessingTime("30 second"))
      .start()

    logger.info("Aggregating from kafka....")
    windowedQuery.awaitTermination()
  }
}
