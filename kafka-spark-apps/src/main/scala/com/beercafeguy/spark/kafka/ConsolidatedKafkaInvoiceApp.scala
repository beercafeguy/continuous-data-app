package com.beercafeguy.spark.kafka

import com.beercafeguy.spark.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.nio.file.{Files, Paths}

object ConsolidatedKafkaInvoiceApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession

    val invoiceStream=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "invoices_flat")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()

    val valueDF = invoiceStream.select(from_avro(col("value"), getSchema()).alias("value"))

    val rewardsDF = valueDF.filter("value.CustomerType == 'PRIME'")
      .groupBy("value.CustomerCardNo")
      .agg(sum("value.TotalValue").alias("TotalPurchase"),
        sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    val kafkaTargetDF=rewardsDF.
      select(expr("CustomerCardNo as key"),
        to_json(struct("*")).as("value"))

    val invoiceWriterQuery = kafkaTargetDF.writeStream
      .queryName("Notification Sender")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("topic", "invoices_agg")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "chk-point-dir/invoices_agg")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    logger.info("Listening to Kafka")
    invoiceWriterQuery.awaitTermination()
  }

  def getSchema():String={
    return new String(Files.readAllBytes(Paths.get("schemas/invoice_items.avsc")))
  }
}
