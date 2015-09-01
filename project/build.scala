import sbt._
import Keys._
import scala.util.Properties

// sbt-assembly
import sbtassembly.Plugin._
import AssemblyKeys._

object Version {
  def either(environmentVariable: String, default: String): String =
    Properties.envOrElse(environmentVariable, default)

  val geotrellis   = "0.10.0-cb236ac"
  val scala        = "2.10.5"
  val scalatest    = "2.2.1"
  lazy val jobserver = either("SPARK_JOBSERVER_VERSION", "0.5.1")
  lazy val hadoop  = either("SPARK_HADOOP_VERSION", "2.6.0")
  lazy val spark   = either("SPARK_VERSION", "1.3.1")
}

object Geoprocessing extends Build {
  // Default settings
  override lazy val settings =
    super.settings ++
  Seq(
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
    version := "0.1.1",
    scalaVersion := Version.scala,
    organization := "org.wikiwatershed.mmw.geoprocessing",

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
    Resolver.bintrayRepo("azavea", "geotrellis"),
    Resolver.bintrayRepo("scalaz", "releases"),
    "OpenGeo" at "http://repo.boundlessgeo.com/main"
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
    base = file(".")).aggregate(summary)

  lazy val summary = Project("summary",  file("summary"))
    .settings(summarySettings:_*)

  lazy val summarySettings =
    Seq(
      organization := "org.wikiwatershed.mmw.geoprocessing",
      name := "mmw-geoprocessing",

      scalaVersion := Version.scala,

      fork := true,
      // raise memory limits here if necessary
      javaOptions += "-Xmx2G",
      javaOptions += "-Djava.library.path=/usr/local/lib",

      libraryDependencies ++= Seq(
        "com.azavea.geotrellis" %% "geotrellis-engine" % Version.geotrellis,
        "com.azavea.geotrellis" %% "geotrellis-services" % Version.geotrellis,
        "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
        "com.azavea.geotrellis" %% "geotrellis-testkit" % Version.geotrellis % "test",
        "org.scalatest" %% "scalatest" % Version.scalatest % "test",
        "org.apache.spark" %% "spark-core" % Version.spark % "provided",
        "org.apache.hadoop" % "hadoop-client" % Version.hadoop % "provided",
        "spark.jobserver" %% "job-server-api" % Version.jobserver % "provided"
      )
    ) ++
  defaultAssemblySettings
}
