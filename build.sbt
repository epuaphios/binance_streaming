ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

val sparkVersion = "3.2.1"


lazy val root = (project in file("."))
  .settings(
    name := "binance_streaming",
      libraryDependencies ++= Seq (
    //  "org.slf4j" % "slf4j-api" % "1.7.5","org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "io.spray" %% "spray-json" % "1.3.6",
      "org.apache.kafka" % "kafka-clients" % "3.2.1",
      "com.typesafe.akka" %% "akka-http" % "10.2.9",
      "com.typesafe.akka" %% "akka-http-testkit" % "10.2.9" % Test,
      "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.10",
      "com.typesafe.akka" %% "akka-actor" % "2.6.19",
      "org.twitter4j" %"twitter4j-stream" %"4.0.7",
      "com.typesafe.play" %% "play-json" % "2.9.3",
      "org.rogach" %% "scallop" % "4.1.0",
      "org.scala-lang" % "scala-actors" % "2.11.12",
      "com.typesafe.akka" %% "akka-stream" % "2.6.19",
      "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1",
      "org.apache.kafka" % "kafka-clients" % "3.2.1"

      ).map(_.exclude ("org.slf4j", "log4j-over-slf4j"))
  )
