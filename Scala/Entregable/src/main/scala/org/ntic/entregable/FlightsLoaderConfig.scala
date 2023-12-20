package org.ntic.entregable

import com.typesafe.config.{Config, ConfigFactory}

object FlightsLoaderConfig {
  /**
   * This object is used to load the configuration file
   */
  val config: Config = ConfigFactory.load()
  //  objeto flightsLoader
  val filePath: String = config.getString("flightsLoader.filePath")
  val hasHeaders: Boolean = config.getBoolean("flightsLoader.hasHeaders")
  val delimiter: String = config.getString("flightsLoader.delimiter")
  val outputDir: String = config.getString("flightsLoader.outputDir")
  val headers: List[String] = config.getStringList("flightsLoader.headers").toArray.map(x => x.asInstanceOf[String]).toList
  val headersLength: Int = headers.length
  val columnIndexMap: Map[String, Int] = headers.map(x => (x, headers.indexOf(x))).toMap
  val filteredOrigin: List[String] = config.getStringList("flightsLoader.filteredOrigin").toArray.map(x => x.asInstanceOf[String]).toList
}
