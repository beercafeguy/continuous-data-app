package com.beercafeguy.spark.streams

import com.beercafeguy.spark.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object FileProcessorApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val spark = SessionBuilder.streamingSession

    val rawDF=spark.readStream
      .format("json")
      .option("maxFilesPerTrigger", 1) //number of files per micro batch
      .option("path","input")
      .load()

    rawDF.printSchema()

    val explodeDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State",
      "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")
    //rawDF.awaitTermination()

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()
  }
}
