package org.ntic.entregable

import java.io.{FileOutputStream, ObjectOutputStream}

object FlightsLoader extends App {

  def writeObject(flights: Seq[Flight], outputFilePath: String): Unit = {
    val out = new ObjectOutputStream(new FileOutputStream(outputFilePath))
    out.writeObject(flights)
    out.close()
  }

  val flights = FileUtils.loadFile(FlightsLoaderConfig.filePath)
  for (origin <- FlightsLoaderConfig.filteredOrigin) {
    val filteredFligths: Seq[Flight] = flights.filter(_.origin.code == origin)
    val delayedFlights: Seq[Flight] = filteredFligths.filter(_.isDelayed).sorted
    val notDelayedFlights: Seq[Flight] = filteredFligths.filterNot(_.isDelayed).sorted

    val flightObjPath: String = s"${FlightsLoaderConfig.outputDir}/$origin.obj"

    val delayedFlightsObj: String = s"${FlightsLoaderConfig.outputDir}/${origin}_delayed.obj"

    writeObject(notDelayedFlights, flightObjPath)

    writeObject(delayedFlights, delayedFlightsObj)
  }
}
