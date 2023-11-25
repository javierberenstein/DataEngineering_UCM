package com.ntic.clases.planner

case class Train(info: trainInfo, schedule: Seq[(Time, Station)]) {
  // Atributo stations
  val stations: Seq[Station] = schedule.map(x => x._2)

  def timeAt(station: Station): Option[Time] =
    schedule.find(stop => stop._2 == station).map(found => found._1)

}
