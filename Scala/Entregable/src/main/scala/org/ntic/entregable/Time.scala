package org.ntic.entregable

case class Time(hours: Int, minutes: Int) extends Ordered[Time] {
  require(hours >= 0 && hours <= 23, "`hours` debe estar entre 0 y 23")
  require(minutes >= 0 && minutes <= 59, "`minutes` debe estar entre 0 y 59")
  val asMinutes = hours*60 + minutes
  override lazy val toString: String = f"$hours%02d:$minutes%02d"

  def minus(that: Time): Int =
    this.asMinutes - that.asMinutes

  def -(that: Time): Int =
    minus(that)

  override def compare(that: Time): Int =
    this - that
}

object Time {

  val totalMinutesInADay = 1440
  def fromString(timeStr: String): Time = {
    val formatted: String = f"${timeStr.toInt}%04d"
    val hours: Int = formatted.substring(0, 2).toInt
    val minutes: Int = formatted.substring(2, 4).toInt
    Time(hours, minutes)
  }

  def fromMinutes(minutes: Int): Time = {
    val normalized = minutes % totalMinutesInADay
    Time(normalized / 60, normalized % 60)
  }
}