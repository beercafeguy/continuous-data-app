package com.beercafeguy.spark.kafka

import com.beercafeguy.spark.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SimpleKafkaInvoiceApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession
    import spark.implicits._

    val invoiceStream=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "simple_invoices")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()

    val valueDF = invoiceStream.select(from_json(col("value").cast("string"), SchemaUtil.getSchema()).alias("value"))
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

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Listening to Kafka")
    invoiceWriterQuery.awaitTermination()
  }
}
