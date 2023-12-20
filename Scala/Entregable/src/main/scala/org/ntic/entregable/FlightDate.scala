package org.ntic.entregable

import com.sun.media.sound.InvalidFormatException

case class FlightDate(day: Int,
                      month: Int,
                      year: Int) {

  lazy val dateString: String = f"$day%02d/$month%02d/$year%02d"

  override def toString: String = dateString
}

object FlightDate {
  def fromString(date: String): FlightDate = {
    /**
     * This function is used to convert a string to a org.ntic.entregable.FlightDate
     * @param date: String
     * @return org.ntic.entregable.FlightDate
     */
    date.split(" ").head.split("/").map(x => x.toInt).toList match {
      case month :: day :: year :: Nil =>
        assert(1 <= day && day <= 31, "Dia inválido")
        assert(1 <= month && month <= 12, "Mes inválido")
        assert(year >= 1987, "Año inválido")
        FlightDate(day, month, year)
      case _ => throw new InvalidFormatException(s"$date tiene un formato inválido")
    }
  }
}
