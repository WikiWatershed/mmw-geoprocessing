import sbt._

object Dependencies {

  private val dependencyScope = "provided"

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % Version.akka
  val akkaHttp = "com.typesafe.akka" %% "akka-http" % Version.akkaHttp
  val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp
  val akkaStream = "com.typesafe.akka" %% "akka-stream" % Version.akka

  val geotrellisSpark = "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis
  val geotrellisS3 = "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis
  val geotrellisRaster = "org.locationtech.geotrellis" %% "geotrellis-raster" % Version.geotrellis
  val geotrellisVector = "org.locationtech.geotrellis" %% "geotrellis-vector" % Version.geotrellis
  val geotrellisSparkTestKit = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit" % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisGdal = "org.locationtech.geotrellis" %% "geotrellis-gdal" % Version.geotrellis

  val pureconfig = "com.github.pureconfig" %% "pureconfig" % "0.9.1"
  val logging = "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  val scalactic = "org.scalactic" %% "scalactic" % Version.scalatest

  val sparkCore = "org.apache.spark" %% "spark-core" % Version.spark % dependencyScope exclude("org.apache.hadoop", "*")
  val sparkSQL = "org.apache.spark" %% "spark-sql" % Version.spark % dependencyScope exclude("org.apache.hadoop", "*")
  val sparkHive = "org.apache.spark" %% "spark-hive" % Version.spark % dependencyScope exclude("org.apache.hadoop", "*")

  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % Version.hadoop % dependencyScope
  val hadoopMapReduceClientCore = "org.apache.hadoop" % "hadoop-mapreduce-client-core" % Version.hadoop % dependencyScope
  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % Version.hadoop % dependencyScope
  val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % Version.hadoop % dependencyScope

  val scalaParallel = "org.scala-lang.modules" %% "scala-parallel-collections" % Version.scalaParallel
}
