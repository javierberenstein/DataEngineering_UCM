package com.ntic.clases.planner

sealed abstract class trainInfo {
  def number: Int
}
case class InterCityExpress(number: Int, hasWifi: Boolean = false) extends trainInfo

case class RegionalExpress(number: Int) extends trainInfo

case class BavarianRegional(number: Int) extends trainInfo
