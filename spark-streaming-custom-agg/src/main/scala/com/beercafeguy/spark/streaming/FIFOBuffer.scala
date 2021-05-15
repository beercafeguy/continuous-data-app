package com.beercafeguy.spark.streaming

import scala.collection.immutable.Queue

case class FIFOBuffer[T](capacity: Int, data: Queue[T] = Queue.empty) extends Serializable {
  def add(element: T): FIFOBuffer[T] = this.copy(data = data.enqueue(element).take(capacity))
  def get: List[T] = data.toList
  def size: Int = data.size
}
