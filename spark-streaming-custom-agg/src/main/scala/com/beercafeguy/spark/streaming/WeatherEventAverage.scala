package com.beercafeguy.spark.streaming

import java.sql.Timestamp

case class WeatherEventAverage(stationId: String,
                               startTime: Timestamp,
                               endTime:Timestamp,
                               pressureAvg: Double,
                               tempAvg: Double)
