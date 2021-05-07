package com.beercafeguy.spark.apps

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SimpleApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleApp")
      .master("local[*]")
      .getOrCreate()

    val surveyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/sample.csv")


    surveyDF.createOrReplaceTempView("survey_tbl")

    val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
    countDF.show(false)
    logger.info(countDF.collect().mkString("->"))
    spark.stop()
  }
}
