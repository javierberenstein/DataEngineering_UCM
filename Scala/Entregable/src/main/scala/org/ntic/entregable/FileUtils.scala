package org.ntic.entregable

import scala.io.Source
import scala.util.Using

object FileUtils {

  def isInvalid(s: String): Boolean = {
    /**
     * This function is used to check if the line is valid or not
     *
     * @param s : String
     * @return Boolean: true if the line is invalid, false otherwise
     */
    val fields = s.split(FlightsLoaderConfig.delimiter)
    s.isEmpty || fields.length != FlightsLoaderConfig.headersLength
  }

  def loadFile(filePath: String): Seq[Flight] = {
    /**
     * This function is used to load the file
     *
     * @param filePath : String
     * @return Seq[Flight]
     */

    Using.Manager { use =>
      val source = use(Source.fromFile(filePath, "UTF-8"))
      val linesList = source.getLines().toList
      val rows = linesList.tail
      val (invalidRows, validRows) = rows.partition(isInvalid)
      val flights = validRows.map(row => Flight.fromString(row))
      flights
    }.getOrElse {
      Seq.empty[Flight]
    }
  }

}
