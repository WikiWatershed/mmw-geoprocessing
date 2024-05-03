import Dependencies._

name := "mmw-geoprocessing"
organization := "org.wikiwatershed"
licenses := Seq(
  "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")
)

scalaVersion := Version.scala
ThisBuild  / scalaVersion := Version.scala
ThisBuild  / version := "6.0.0"

lazy val root = Project("mmw-geoprocessing", file("."))
  .aggregate(
    api
  )

lazy val api = project
  .settings(commonSettings ++ apiDependencies ++ consoleSettings)

lazy val scalacOpts = Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-Yrangepos"
)

lazy val apiDependencies = Seq(
  libraryDependencies ++= Seq(
    akkaActor,
    akkaHttp,
    akkaHttpSprayJson,
    akkaStream,
    logging,
    scalaParallel,
    scalatest % Test,
    scalactic % Test,
    geotrellisS3,
    geotrellisGdal,
    geotrellisRaster,
    geotrellisVector
  )
)

lazy val commonSettings = Seq(
  evictionErrorLevel := Level.Warn,

  Compile / scalacOptions ++= scalacOpts,
  Compile / console / scalacOptions -= "-Ywarn-unused-import",
  unmanagedSources / excludeFilter := ".#*.scala",
  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := { _ =>
    false
  },

  resolvers ++= Seq(
    "GeoSolutions" at "https://maven.geo-solutions.it/",
    "LT-releases" at "https://repo.locationtech.org/content/groups/releases",
    "OSGeo" at "https://repo.osgeo.org/repository/release/",
    "maven2" at "https://repo1.maven.org/maven2"
  ),

  assembly / assemblyShadeRules := {
    val shadePackage = "org.wikiwatershed.shaded"
    Seq(
      ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
    )
  },

  Test / fork := true,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oD"),
  Test / javaOptions ++= Seq("-Xms1024m", "-Xmx8144m", "-Djts.overlay=ng"),

  // Settings for sbt-assembly plugin which builds fat jars for use by spark jobs
  assembly / test := {},
  assembly / assemblyMergeStrategy  := {
    case "reference.conf" => MergeStrategy.concat
    case "application.conf" => MergeStrategy.concat
    case PathList("META-INF", xs@_*) =>
      xs match {
        case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
        // Concatenate everything in the services directory to keep GeoTools happy.
        case ("services" :: _ :: Nil) =>
          MergeStrategy.concat
        // Concatenate these to keep JAI happy.
        case ("javax.media.jai.registryFile.jai" :: Nil) |
             ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
          MergeStrategy.concat
        case (name :: Nil) => {
          // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
          if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(
            ".SF"
          ))
            MergeStrategy.discard
          else
            MergeStrategy.first
        }
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  }
)

lazy val consoleSettings = Seq(
  // auto imports for local SBT console
  // can be used with `test:console` command
  console / initialCommands :=
    """
  import geotrellis.raster._
  import geotrellis.vector._
  import geotrellis.vector.io._
  import geotrellis.vector.io.wkt.WKT
  """
)
