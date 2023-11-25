package com.ntic.clases.planner

class JourneyPlanner(trains: Set[Train]) {
  val stations: Set[Station] = trains.flatMap(train => train.stations)

  def trainsAt(station: Station): Set[Train] =
    trains.filter(trains => trains.stations.contains(station))

  def stopsAt(station:Station): Set[(Time, Train)] = {
    for {
      // Por cada train
      train <- trains
      // Por cada tupla time, station de la secuencia schedule del objeto train, asegurnado que la parada
      // corresponde con la estación
      (time, stop) <- train.schedule if stop == station
      // Tambien se puede acceder a la tupla con un patron
      // (time, `station`) <- train.schedule
      // Devuelve la tupla. Como estamos iterando sobre un set, el resultado será un Set
    } yield (time, train)
  }

  def isShortTrip(from: Station, to: Station): Boolean = {
    // Para todos los trenes, verifica si existe
    trains.exists(train =>
      // Itera sobre estaciones, descartando las estaciones que no son equivalentes a from
      train.stations.dropWhile(station => station != from) match {
        // Verifica si despues de from (+:) está el to
        case `from` +: `to` +: _ => true
        // Verifica si despues de from, hay algo,y después está el to
        case `from` +: _ +: `to` +: _ => true
        case _ => false
      }
    )
  }




}
