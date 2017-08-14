package org.wikiwatershed.mmw.geoprocessing

import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.http.scaladsl.server.{ HttpApp, Route }
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

case class InputData(
  operationType: String,
  rasters: List[String],
  zoom: Int,
  polygonCRS: String,
  rasterCRS: String,
  polygon: List[String]
)

case class PostRequest(input: InputData)
case class Result(result: Map[String, Int])

object PostRequestProtocol extends DefaultJsonProtocol {
  implicit val inputFormat = jsonFormat6(InputData)
  implicit val postFormat = jsonFormat1(PostRequest)
  implicit val resultFormat = jsonFormat1(Result)
}

object WebServer extends HttpApp with App with LazyLogging with Geoprocessing {
  import PostRequestProtocol._

  def routes: Route =
    get {
      path("ping") {
        complete("pong")
      }
    } ~
    post {
      path("run") {
        entity(as[PostRequest]) { data =>
          data.input.operationType match {
            case "RasterGroupedCount" =>
              complete(getRasterGroupedCount(data.input))
            case _ =>
              throw new Exception(s"Unknown operationType: ${data.input.operationType}")
          }
        }
      }
    }

  val config = ConfigFactory.load()
  val port = config.getInt("geoprocessing.port")
  val host = config.getString("geoprocessing.hostname")

  startServer(host, port)
}
