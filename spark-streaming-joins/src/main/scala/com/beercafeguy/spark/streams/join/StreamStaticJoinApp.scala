package com.beercafeguy.spark.streams.join

import com.beercafeguy.spark.streams.commons.{BeerCafeStreamingSessionBuilder, SchemaUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, max, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object StreamStaticJoinApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val spark=BeerCafeStreamingSessionBuilder.sparkSession
    import spark.implicits._

    val invoiceStream=spark
      .readStream //change to read to make it a simple batch app
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "logins")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .load()



    val valueDF = invoiceStream.select(
      from_json(col("value").cast("string"), SchemaUtil.loginSchema).alias("value"))
      .select("value.*")

    //valueDF.printSchema()
    val loginDF=valueDF
      .withColumn("login_time",to_timestamp($"login_time","yyyy-MM-dd HH:mm:ss"))

    val userDF=spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "dbtest")
      .option("table", "users")
      .load()

    //userDF.printSchema()

    val joinExpr = loginDF.col("login_id") === userDF.col("login_id")
    val joinType = "inner"
    val joinedDF = loginDF.join(userDF, joinExpr, joinType)
      .drop(loginDF.col("login_id"))

    val outputDF = joinedDF.select(col("login_id"), col("user_name"),
      col("login_time").alias("last_login"))


    val outputQuery = outputDF.writeStream
      .foreachBatch(writeToCassandra _)
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir/cassandra_sink")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()


    logger.info("Waiting for Query")
    outputQuery.awaitTermination()
  }

  def writeToCassandra(outputDF: DataFrame, batchID: Long): Unit = {
    outputDF.cache
    outputDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "dbtest")
      .option("table", "users")
      .mode("append")
      .save()

    outputDF.show()
  }
}
