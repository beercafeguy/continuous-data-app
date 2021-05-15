package com.beercafeguy.spark.streaming

import java.sql.Timestamp

case class WeatherEvent(stationId: String, timestamp: Timestamp, location:(Double,Double), pressure: Double, temp: Double)
