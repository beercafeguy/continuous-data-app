package com.beercafeguy.spark.apps
import org.apache.spark.sql.SparkSession

object DatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Datasets Spark App")
      .config("spark.sql.shuffle.partitions",4)
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    case class Usage(uid:Int,uname:String,usage:Int)
    val random=new scala.util.Random(42)
    val data=for (i <- 0 to 1000)
      yield (Usage(i,"user-"+random.alphanumeric.take(5).mkString(""),random.nextInt(1000)))

    val dsUsage=spark.createDataset(data)
    dsUsage.show()
  }
}
