package com.beercafeguy.spark.streams.join

import com.beercafeguy.spark.streams.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.Trigger

object StreamStreamJoinApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession
    import spark.implicits._

    val rawImpressionDF=spark
      .readStream //change to read to make it a simple batch app
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "impressions")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()



    val impressionDF:DataFrame = rawImpressionDF.select(
      from_json(col("value").cast("string"), SchemaUtil.impressionSchema).alias("value"))
      .select("value.*")
      .withColumn("ImpressionId",$"InventoryID")
      .withColumn("ImpressionTime",to_timestamp($"CreatedTime","yyyy-MM-dd HH:mm:ss"))
      .drop("InventoryID","CreatedTime")

    val rawClickDF=spark
      .readStream //change to read to make it a simple batch app
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "clicks")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()
    //valueDF.printSchema()
    val clickDF:DataFrame=rawClickDF
      .select(from_json(col("value").cast("string"), SchemaUtil.clickSchema).alias("value"))
      .select("value.*")
      .withColumn("ClickTime",to_timestamp($"CreatedTime","yyyy-MM-dd HH:mm:ss"))
      .select($"InventoryID".as("ClickId"),$"ClickTime")

    //userDF.printSchema()

    val joinExpr = impressionDF("ImpressionId")===clickDF("ClickId")
    val joinType = "inner"
    val joinedDF = impressionDF.join(clickDF, joinExpr, joinType)

    val outputQuery = joinedDF.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/stream_stream")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Waiting for Query")
    outputQuery.awaitTermination()
  }
}
