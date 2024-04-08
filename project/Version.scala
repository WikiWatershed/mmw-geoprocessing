import sbt._
import scala.util.Properties

object Version {
  def either(environmentVariable: String, default: String): String =
    Properties.envOrElse(environmentVariable, default)

  val akkaHttp      = "10.5.3"
  val akka          = "2.8.5"
  val geotrellis    = "3.7.0"
  val hadoop        = "3.2.1"
  val scala         = either("SCALA_VERSION", "2.13.12")
  val scalaLogging  = "3.9.5"
  val scalaParallel = "1.0.4"
  val scalatest     = "3.2.18"
  val spark         = either("SPARK_VERSION", "3.3.3")
}
