package com.beercafeguy.spark.streaming

import java.sql.Timestamp

case class Rate(timestamp: Timestamp, value: Long)
