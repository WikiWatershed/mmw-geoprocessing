import Version._

name := "mmw-geoprocessing-summary"

scalaVersion := Version.scala

fork := true
// raise memory limits here if necessary
javaOptions += "-Xmx2G"
javaOptions += "-Djava.library.path=/usr/local/lib"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis,
  "org.apache.spark" %% "spark-core" % Version.spark % "provided",
  "org.apache.hadoop" % "hadoop-client" % Version.hadoop % "provided",
  "spark.jobserver" %% "job-server-api" % Version.jobserver % "provided",
  "commons-io" % "commons-io" % "2.5"
)
