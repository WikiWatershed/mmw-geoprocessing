name := "mmw-geoprocessing-api"

scalaVersion := Version.scala

fork := true
// raise memory limits here if necessary
javaOptions += "-Xmx2G"
javaOptions += "-Djava.library.path=/usr/local/lib"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis,
  "com.typesafe.akka"     %% "akka-actor"                        % Version.akka,
  "com.typesafe.akka"     %% "akka-http-experimental"            % Version.akka,
  "com.typesafe.akka"     %% "akka-http-spray-json-experimental" % Version.akka,
  "org.apache.spark" %% "spark-core" % Version.spark % "provided",
  "org.apache.hadoop" % "hadoop-client" % Version.hadoop % "provided",
  "spark.jobserver" %% "job-server-api" % Version.jobserver % "provided"
)
