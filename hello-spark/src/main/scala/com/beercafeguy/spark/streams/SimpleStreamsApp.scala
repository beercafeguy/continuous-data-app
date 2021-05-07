package com.beercafeguy.spark.streams

import com.beercafeguy.spark.commons.SessionBuilder
import com.beercafeguy.spark.commons.SessionBuilder.streamingSession.implicits._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.streaming.OutputMode

object SimpleStreamsApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
  val spark=SessionBuilder.streamingSession


    val linesDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()

    linesDF.printSchema()
    val wordsDF=linesDF.select(explode(split($"value"," ")).as("word"))
    val countDF=wordsDF.groupBy("word").count()
    val streamingQuery=countDF.writeStream
      .format("console")
      .option("checkpointLocation","check_point_dir")
      .outputMode(OutputMode.Complete())
      .start()
    logger.info("Looking for data @localhost:9999")
    streamingQuery.awaitTermination()
  }
}
