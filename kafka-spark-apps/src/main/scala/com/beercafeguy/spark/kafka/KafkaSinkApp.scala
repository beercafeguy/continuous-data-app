package com.beercafeguy.spark.kafka

import com.beercafeguy.spark.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json, to_json,struct}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object KafkaSinkApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession
    import spark.implicits._

    val invoiceStream=spark
      .readStream //change to read to make it a simple batch app
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "simple_invoices")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()

    val valueDF = invoiceStream.select(from_json(col("value").cast("string"), SchemaUtil.getSchema()).alias("value"))
    val notificationDF=valueDF.select("value.InvoiceNumber","value.CustomerCardNo","value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints",$"TotalAmount"*0.2)
    //notificationDF.printSchema()

    /*val kafkaTargetDF=notificationDF.selectExpr(
      "InvoiceNumber as key",
      """
        |to_json(named_struct('CustomerCardNo',CustomerCardNo,
        |'TotalAmount',TotalAmount,
        |'EarnedLoyaltyPoints',EarnedLoyaltyPoints
        |)) as value
        |""".stripMargin
    )*/

    val kafkaTargetDF=notificationDF.selectExpr("InvoiceNumber as key", "to_json(struct(*)) AS value")

    val notificationWriterQuery=kafkaTargetDF.writeStream
      .queryName("Notification Sender")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("topic", "loyalty_notifications")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Listening to invoices and writing notifications......")
    notificationWriterQuery.awaitTermination()
  }
}
