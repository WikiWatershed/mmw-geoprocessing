import sbt._
import Keys._
import scala.util.Properties

// sbt-assembly
import sbtassembly.Plugin._
import AssemblyKeys._

object Version {
  def either(environmentVariable: String, default: String): String =
    Properties.envOrElse(environmentVariable, default)

  val geotrellis   = "1.1.1"
  val geotrellisOld = "0.10.0"
  val scala        = either("SCALA_VERSION", "2.11.11")
  val scalatest    = "2.2.1"
  lazy val jobserver = either("SPARK_JOBSERVER_VERSION", "0.6.1")
  lazy val hadoop  = either("SPARK_HADOOP_VERSION", "2.6.0")
  lazy val spark   = either("SPARK_VERSION", "1.5.2")
  lazy val akkaHttpVersion = "10.0.9"
  lazy val akkaVersion    = "2.4.16"
  lazy val akkaHttpCorsVersion = "0.2.1"
  lazy val scalaLoggingVersion = "3.7.2"
  lazy val sparkCoreVersion = "2.1.1"
}

object Geoprocessing extends Build {
  // Default settings
  override lazy val settings =
    super.settings ++
  Seq(
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
    version := "4.0.1",
    scalaVersion := Version.scala,
    organization := "org.wikiwatershed.mmw.geoprocessing",
    name := "mmw-geoprocessing",

    scalaVersion := Version.scala,
    fork := true,

    // raise memory limits here if necessary
    javaOptions += "-Xmx2G",
    javaOptions += "-Djava.library.path=/usr/local/lib",

    // disable annoying warnings about 2.10.x
    conflictWarning in ThisBuild := ConflictWarning.disable,
    scalacOptions ++=
      Seq("-deprecation",
        "-unchecked",
        "-Yinline-warnings",
        "-language:implicitConversions",
        "-language:reflectiveCalls",
        "-language:higherKinds",
        "-language:postfixOps",
        "-language:existentials",
        "-feature"),

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
  )

  val resolutionRepos = Seq(
    Resolver.bintrayRepo("scalaz", "releases"),
    "OpenGeo" at "https://boundless.artifactoryonline.com/boundless/main",
    "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
  )

  val defaultAssemblySettings =
    assemblySettings ++
  Seq(
    test in assembly := {},
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case "reference.conf" => MergeStrategy.concat
        case "application.conf" => MergeStrategy.concat
        case "META-INF/MANIFEST.MF" => MergeStrategy.discard
        case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    },
    resolvers ++= resolutionRepos
  )

  lazy val root = Project(id = "mmw-geoprocessing",
    base = file(".")).aggregate(api, summary)

  lazy val api = Project("api", file("api"))
    .settings(apiSettings:_*)

  lazy val summary = Project("summary", file("summary"))
    .settings(summarySettings:_*)

  lazy val apiSettings =
    Seq(
      libraryDependencies ++= Seq(
        "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
        "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis,
        "com.typesafe.akka" %% "akka-actor" % Version.akkaVersion,
        "com.typesafe.akka" %% "akka-http" % Version.akkaHttpVersion,
        "com.typesafe.akka" %% "akka-stream" % Version.akkaVersion,
        "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttpVersion,
        "org.scalatest" %% "scalatest" % Version.scalatest % "test",
        "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLoggingVersion,
        "org.apache.spark" %% "spark-core" % Version.sparkCoreVersion,
        "org.typelevel" %% "cats-core" % "1.0.1"
      )
    ) ++
  defaultAssemblySettings

  lazy val summarySettings =
    Seq(
      libraryDependencies ++= Seq(
        "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellisOld,
        "com.azavea.geotrellis" %% "geotrellis-s3" % Version.geotrellisOld,
        "org.apache.spark" %% "spark-core" % Version.spark % "provided",
        "org.apache.hadoop" % "hadoop-client" % Version.hadoop % "provided",
        "spark.jobserver" %% "job-server-api" % Version.jobserver % "provided"
      )
    ) ++
  defaultAssemblySettings
}
