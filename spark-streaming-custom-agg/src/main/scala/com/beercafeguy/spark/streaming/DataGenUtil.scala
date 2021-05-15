package com.beercafeguy.spark.streaming

object DataGenUtil {

  val locationGenerator: () => (Double, Double) = {
    // Europe bounds
    val longBounds = (-10.89,39.82)
    val latBounds = (35.52,56.7)
    def pointInRange(bounds:(Double, Double)): Double = {
      val (a, b) = bounds
      Math.abs(scala.util.Random.nextDouble())*b+a
    }
    () => (pointInRange(longBounds), pointInRange(latBounds))
  }

  def pickOne[T](list: List[T]): T = list(scala.util.Random.nextInt(list.size))
  val pressureGen: () => Double = () => scala.util.Random.nextDouble + 101.0
  val tempGen: () => Double = () => scala.util.Random.nextDouble * 60 - 20

}
