  // Default settings
lazy val commonSettings = Seq(
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
  version := "1.2.0",
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
  licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  resolvers ++= Seq(
    Resolver.bintrayRepo("scalaz", "releases"),
    Resolver.bintrayRepo("spark-jobserver", "maven"),
    "OpenGeo" at "https://boundless.artifactoryonline.com/boundless/main"
  )
)


  // val defaultAssemblySettings = assemblySettings ++
  // Seq(
  //   test in assembly := {},
  //   mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  //     (old) => {
  //       case "reference.conf" => MergeStrategy.concat
  //       case "application.conf" => MergeStrategy.concat
  //       case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  //       case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  //       case _ => MergeStrategy.first
  //     }
  //   },
  //   resolvers ++= resolutionRepos
  // )

lazy val root = Project("mmw-geoprocessing", file(".")).aggregate(summary, api)

lazy val summary = Project("summary",  file("summary"))
  .settings(commonSettings:_*)

lazy val api = Project("api",  file("api"))
  .settings(commonSettings:_*).dependsOn(summary)
