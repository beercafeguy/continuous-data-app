package com.beercafeguy.spark.streaming

import org.apache.spark.sql.streaming.GroupState

import java.sql.Timestamp

object BasicUtils {



  def mappingFunction(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]): WeatherEventAverage = {
    val ElementCountWindowSize = 10
    // get current state or create a new one if there's no previous state
    val currentState = state.getOption.getOrElse(new FIFOBuffer[WeatherEvent](ElementCountWindowSize))
    // enrich the state with the new events
    val updatedState = values.foldLeft(currentState){case (st, ev) => st.add(ev)}
    // update the state with the enriched state
    state.update(updatedState)
    // if we have enough data, create a WeatherEventAverage from the accumulated state
    // otherwise, make a zeroed record
    val data = updatedState.get
    if (data.size > 2) {
      val start = data.head
      val end = data.last
      val pressureAvg = data.map(event => event.pressure).sum/data.size
      val tempAvg = data.map(event => event.temp).sum/data.size
      WeatherEventAverage(key, start.timestamp, end.timestamp, pressureAvg, tempAvg)
    } else {
      WeatherEventAverage(key, new Timestamp(0), new Timestamp(0), 0.0, 0.0)
    }
  }
}
