ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"
// ThisBuild / assemblyPrependShellScript := Some(defaultShellScript)


val mainClassName = "FlightsLoader"


lazy val root = (project in file("."))
  .settings(
    name := "Entregable",

    Compile / run / mainClass := Some(mainClassName),
    assembly / mainClass := Some(mainClassName),
    assembly / assemblyJarName := "flights_loader.jar",

    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.2",
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scala-lang" %% "toolkit-test" % "0.1.7" % Test
    )
  )
