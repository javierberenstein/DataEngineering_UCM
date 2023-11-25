package com.ntic.clases.planner
import scala.util.Try

object Time {
  def fromMinutes(totalMinutes: Int): Time = {
    val hours = (totalMinutes / 60) % 24 // Normalize hours to be between 0 and 23
    val minutes = totalMinutes % 60 // Normalize minutes to be between 0 and 59
    Time(hours, minutes)
  }

  def fromMap(m: Map[String, String]): Option[Time] = {
    // Option 1
    // val hours = Try(m("hours").toInt)
    // val t: String = hours.map(h => Try(m("minutes").toInt).map(m => com.ntic.clases.journeyplanner.Time(h, m)))
    // val t: String = hours.flatMap(h => Try(m("minutes").toInt).map(m => com.ntic.clases.journeyplanner.Time(h, m)))
    // Option 2
    //    for {
    //      hours <- Try(m("hours").toInt)
    //      minutes <- Try(m("minutes").toInt) match {
    //        case Success(value) => Success(value)
    //        case Failure(_) => Success(0)
    //      }
    //    } yield com.ntic.clases.scalaTrain.Time(hours, minutes)
    // Option 3
    val tryTime = for {
      hours <- Try(m("hours").toInt)
      minutes <- Try(m("minutes").toInt).recover({ case _: Exception => 0 })
    } yield Time(hours, minutes)
    tryTime.toOption
  }
}

case class Time(hours: Int = 0, minutes: Int = 0) extends Ordered[Time] {

  require(hours >= 0 && hours < 24, "Hours must be between 0 and 23")
  require(minutes >= 0 && minutes < 60, "Minutes must be between 0 and 59")

  // Atributo extendido ordered
  def compare(that: Time) = this.asMinutes - that.asMinutes

  // Atributo de la clase
  val asMinutes = hours*60 + minutes

  // Resta de tiempo
  def minus(time: Time): Int = this.asMinutes - time.asMinutes

  // Operador the resta, invoca a minus
  def -(time: Time): Int = minus(time)

  override lazy val toString: String =
    f"$hours%02d:$minutes%02d"
}
