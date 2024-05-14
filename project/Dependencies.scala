import sbt._

object Dependencies {

  private val dependencyScope = "provided"

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % Version.akka
  val akkaHttp = "com.typesafe.akka" %% "akka-http" % Version.akkaHttp
  val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp
  val akkaStream = "com.typesafe.akka" %% "akka-stream" % Version.akka

  val geotrellisS3 = "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis
  val geotrellisRaster = "org.locationtech.geotrellis" %% "geotrellis-raster" % Version.geotrellis
  val geotrellisVector = "org.locationtech.geotrellis" %% "geotrellis-vector" % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisGdal = "org.locationtech.geotrellis" %% "geotrellis-gdal" % Version.geotrellis

  val geotrellisStac = "com.azavea.geotrellis" %% "geotrellis-stac" % Version.geotrellisStac

  val kindProjector = "org.typelevel" %% "kind-projector" % Version.kindProjector cross CrossVersion.full

  val stac4s = "com.azavea.stac4s" %% "core" % Version.stac4s
  val stac4sClient = "com.azavea.stac4s" %% "client" % Version.stac4s
  val sttp = "com.softwaremill.sttp.client3" %% "core" % Version.sttp
  val sttpAkkaBackend = "com.softwaremill.sttp.client3" %% "akka-http-backend" % Version.sttp

  val pureconfig = "com.github.pureconfig" %% "pureconfig" % "0.9.1"
  val logging = "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  val scalactic = "org.scalactic" %% "scalactic" % Version.scalatest

  val scalaParallel = "org.scala-lang.modules" %% "scala-parallel-collections" % Version.scalaParallel
}
