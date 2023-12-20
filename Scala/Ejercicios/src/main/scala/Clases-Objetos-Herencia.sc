// Define una clase: Tiempo con propiedades de sólo lectura: horas y minutos y
//un método: esAnterior(t: Tiempo): Boolean, que verifique si el objeto tiempo
//pasado por parámetro es anterior.

// El hecho de pasar los argumentos como val los hace inmutables y solo lectura
class Time(val hours: Int, val minutes: Int) {

  require(hours >= 0 && hours < 24, "Horas deben estar entre 0 y 23.")
  require(minutes >= 0 && minutes < 60, "Minutos deben estar entre 0 y 59.")

  def isBefore(that: Time): Boolean = {
    this.hours < that.hours || (this.hours == that.hours && this.minutes < that.minutes)
  }
}

val firstTime = new Time(1, 6)
val secondTime = new Time(6, 23)

firstTime.isBefore(secondTime)

//Define una clase: Persona cuyo constructor acepte 2 parámeteros, nombre:
//String y edad: Int y una función imprime: Unit, que use la función println para
//escribir un mensaje por consola mostrando los valores de nombre y edad en el
//siguiente formato: "Mi nombre es $nombre y tengo $edad"

class Person(name: String, age: Int) {
  def print: Unit = println(s"Mi name is $name and I'm $age years old")
}

val yo = new Person("Javier", 28)
yo.print

// Define una clase: Contador que tenga una propiedad: valor y dos funciones:
//incrementar: Unit y actual: Int. Incrementar suma 1 a la propiedad valor y actual
//devuelve el valor- Define una función extra esMenor(c: Counter):
//Boolean que determine si el valor de c es menor que el valor del contador
//actual.

class Counter(initial_value: Int) {

  private var value = initial_value

  def increment(): Unit = {
    this.value += 1
  }
  def getValue(): Int  = {
    this.value
  }

  def isLesser(c: Counter): Boolean = {
    this.getValue() < c.getValue()
  }

}

val counter = new Counter(6)

counter.increment()
counter.increment()
counter.getValue()

val another_counter = new Counter(2)

counter.isLesser(another_counter)





