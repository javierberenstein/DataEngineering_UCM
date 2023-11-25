// Escribir en Scala el código equivalente a:
//for (int i=10; i>=0; i--) println(i)

for (i <-10 to 0 by -1) {
  println(i)
}
(0 to 10).reverse.foreach(println)
// Map retorna una lista nueva, vacia en este caso.
(0 to 10 ).reverse.map(println)

// Definir una función cuya firma es: countDown(n: Int): Unit. Esta función
//imprimirá los números de n a 0

def countDown(n: Int): Unit = {
  if (n >= 0) {
    println(n)
    countDown(n - 1)
  }
}
countDown(10)

// Escribir el código que asigne a la variable 'a' una colección (da igual si se
//define para, Seq, List, Array) de n enteros aleatorios entre 0 (incluído) y n
//(excluido)

import scala.util.Random

val a: Seq[Int] = 0 to Random.nextInt(10)

// Dado una colección de enteros, se pide generar una nueva colección que
//contenga todos los números positivos de la colección original en el orden de la
//primera colección seguidos por los ceros y negativos, todos en su orden
//original

val l1: Seq[Int] = List.apply(0, -1, 2, 3, -6, 4, -2, -2)

val l_res_1: Seq[Int] = l1.filter(_ >= 0) ++ l1.filter(_ < 0)
val l_res_12: Seq[Int] = l1.filter(_ >= 0).concat(l1.filter(_ < 0))

// Definir una función que calcule la media de un Array[Double]

val ArrayDouble: Array[Double] = Array.apply(1.3, 5.2, 9.73, 5,7, 6,28)

def media(array: Array[Double]): Double = {
  if (array.isEmpty) 0.0
  else array.sum / array.length
}

media(ArrayDouble)

// Definir una función que reciba un argumento de tipo Array[Int] y devuelva un
//Array[Int] sin duplicados.

val notUniqueInt: Array[Int] = Array.apply(1, 4, 4, 5, 1, 2, 5, 7)

// No garantiza el orden, es más eficiente para conjunto de datos grandes
def unique(array: Array[Int]): Array[Int] = {
  array.toSet.toArray
}

// Garantiza el orden, menor eficiencia en conjunto de datos grandes
def unique_2(array: Array[Int]): Array[Int] = {
  array.distinct
}

unique(notUniqueInt)
unique_2(notUniqueInt)

// Definir una función que reciba un argumento de tipo Map[String, Int] y produzca
//un Map[String, Int] manteniendo las mismas claves, pero con los valores
//incrementados en 100.

val myMap: Map[String, Int] = Map("a"->10, "b"->20, "c"->100, "d"->200)

// Tuple iterating, not elegant
def plus_100(map: Map[String, Int]): Map[String, Int] = {
  map.map(kv => (kv._1, kv._2 + 100))
}
// Pattern matching, more Scala idiomatic
def plus_100_2(map: Map[String, Int]): Map[String, Int] = {
  map.map { case (clave, valor) => clave -> (valor + 100) }
}
// Eficiente para Maps muy grandes
def plus_100_3(map: Map[String, Int]): Map[String, Int] = {
  map.view.mapValues(_ + 100).toMap
}
// idiomatico, eficiente y elegante, underlaying case is performed (plus_100_2)
def plus_100_4(map: Map[String, Int]): Map[String, Int] = {
  map.transform((_, value) => value + 100)
}
// - eficiencia + legibilidad
def plus_100_5(map: Map[String, Int]): Map[String, Int] = {
  (for (key <- map.keys) yield key -> (map(key) + 100)).toMap
}

plus_100(myMap)
plus_100_2(myMap)
plus_100_3(myMap)
plus_100_4(myMap)
plus_100_5(myMap)

// Definir una función que reciba una colección: minmax(values: Array[Int]) que
//devuelva un par (tupla) con el menor y mayor valor del array.

def minmax()



