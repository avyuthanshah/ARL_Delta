import Dependencies._

ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "deltaF",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "com.crealytics" %% "spark-excel" % "3.4.2_0.20.3",
      "io.delta" %% "delta-spark" % "3.1.0",
      "mysql" % "mysql-connector-java" % "8.0.33",

//      "com.typesafe.akka" %% "akka-actor" % "2.8.5",
      "org.apache.hadoop" % "hadoop-client" % "3.3.6",
      "org.apache.hadoop" % "hadoop-client-api" % "3.3.6",
      "org.apache.hadoop" % "hadoop-hdfs" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
      
      //kafka Integration
      "org.apache.kafka" %% "kafka" % "3.7.0",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      "org.apache.kafka" % "kafka-streams" % "3.7.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",

      "com.github.oshi" % "oshi-core" % "6.6.1",

        munit % Test
    )
  )

ThisBuild / javacOptions ++= Seq(
  "--release", "11"
)

ThisBuild / scalacOptions ++= Seq(
  "-release", "11"
)

