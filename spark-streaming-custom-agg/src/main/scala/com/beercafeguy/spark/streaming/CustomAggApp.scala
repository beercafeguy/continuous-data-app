package com.beercafeguy.spark.streaming

import com.beercafeguy.spark.streaming.DataGenUtil.{locationGenerator, pickOne, pressureGen, tempGen}
import org.apache.spark.sql.functions.{from_json, to_timestamp}
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object CustomAggApp {

  def main(args: Array[String]): Unit = {
    val spark=BasicSessionBuilder.spark
    import spark.implicits._

    val rate = spark
      .readStream
      .format("rate")
      //.format("kafka")
      //.option("kafka.bootstrap.servers", "localhost:9093")
      //.option("subscribe", "rates")
      //.option("startingOffsets", "earliest")
      //.option("failOnDataLoss","false")
      .load()
      //.select(from_json($"value".cast("string"),rateSchema).as("value"))
      //.select("value.*")
      //.withColumn("ts",to_timestamp($"ts","yyyy-MM-dd HH:mm:ss"))
      .as[Rate]
    val weatherEvents = rate.map{case Rate(ts, value) => WeatherEvent(pickOne(uids), ts, locationGenerator(), pressureGen(), tempGen())}

    val weatherEventsMovingAverage = weatherEvents.groupByKey(record => record.stationId)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(BasicUtils.mappingFunction)

    val outQuery = weatherEventsMovingAverage.writeStream
      .format("memory")
      .queryName("weatherAverage")
      .outputMode("update")
      .start()

    outQuery.stop()
    val table  = spark.sql("select * from weatherAverage where pressureAvg == 0.0")
    table.show(truncate= false)
  }

  val uids = List("d1e46a42", "d8e16e2a", "d1b06f88",
    "d2e710aa", "d2f731cc", "d4c162ee",
    "d4a11632", "d7e277b2", "d59018de",
    "d60779f6")

  val rateSchema=new StructType()
    .add(StructField("ts",StringType))
    .add(StructField("value",LongType))
}
