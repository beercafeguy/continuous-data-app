package com.beercafeguy.spark.kafka

import com.beercafeguy.spark.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object MultiStreamKafkaApp {

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
      .option("groupIdPrefix","Spark_Invoice_Notification_App")
      .load()

    val valueDF = invoiceStream.select(from_json(col("value").cast("string"), SchemaUtil.getSchema()).alias("value"))
    val notificationDF=valueDF.select("value.InvoiceNumber","value.CustomerCardNo","value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints",$"TotalAmount"*0.2)

    val kafkaTargetDF=notificationDF.selectExpr("InvoiceNumber as key", "to_json(struct(*)) AS value")

    //First query to write to Kafka
    val notificationWriterQuery=kafkaTargetDF.writeStream
      .queryName("Notification Sender")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("topic", "loyalty_notifications")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "chk-point-dir/kafka")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    // Second query to write to json invoice in file
    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir/json")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Listening to invoices and writing notifications......")
    spark.streams.awaitAnyTermination()
  }

}
