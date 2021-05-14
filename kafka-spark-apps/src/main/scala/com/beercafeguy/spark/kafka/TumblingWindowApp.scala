package com.beercafeguy.spark.kafka

import com.beercafeguy.spark.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, from_json, lower, sum, to_timestamp, when, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object TumblingWindowApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession
    import spark.implicits._

    val invoiceStream=spark
      .readStream //change to read to make it a simple batch app
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "market_trades")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss","false")
      .load()

    val valueDF = invoiceStream.select(from_json(col("value").cast("string"), SchemaUtil.stockSchema).alias("value")).select("value.*")

    //valueDF.printSchema()
    val mappedDF=valueDF
      .withColumn("CreatedTime",to_timestamp($"CreatedTime","yyyy-MM-dd HH:mm:ss"))
      .withColumn("Buy",when(lower($"Type")==="buy",$"Amount").otherwise(0.0))
      .withColumn("Sell",when(lower($"Type")==="sell",$"Amount").otherwise(0.0))
      .drop("Type","Amount")
    //mappedDF.printSchema()

    val windowedDF=mappedDF
      .withWatermark("CreatedTime","30 minute")
      .groupBy(window($"CreatedTime","15 minute"))
    val resultDF=windowedDF.agg(sum($"Buy").as("Total_Buy"),sum($"Sell").as("Total_Sell"))
      .select($"window.start",$"window.end",$"Total_Buy",$"Total_Sell")
    resultDF.printSchema()

    //Following is not alowed in spark structured streaming but can be tested in dev
    /*
    val runningTotalWindowSpec=Window.orderBy($"end")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val outputDF=resultDF
      .withColumn("RTotalBuy",sum($"Total_Buy") over(runningTotalWindowSpec))
      .withColumn("RTotalSell",sum($"Total_Sell") over(runningTotalWindowSpec))
      .withColumn("NetValue",$"RTotalBuy"-$"RTotalSell")
    outputDF.show(false)
    */
    val windowedQuery=resultDF.writeStream
      .format("console")
      .outputMode("update")
      // in complete mode the whole state will be sent to output -> no cleanup of state store due to water mark
      // so watermark will be overridden by complete mode
      // In Append -> not allowed in aggregates without watermark (will only emit record to sink if it won't update in future)
      .option("checkpointLocation", "chk-point-dir/tumb_window")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Aggregating from kafka....")
    windowedQuery.awaitTermination()

  }
}
