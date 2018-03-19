package org.wikiwatershed.mmw.geoprocessing

import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.http.scaladsl.server.{ HttpApp, Route }
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

case class InputData(
  operationType: String,
  rasters: List[String],
  targetRaster: Option[String],
  pixelIsArea: Option[Boolean],
  zoom: Int,
  polygonCRS: String,
  rasterCRS: String,
  polygon: List[String],
  vectorCRS: Option[String],
  vector: Option[List[String]]
)

case class PostRequest(input: InputData)
case class ResultInt(result: Map[String, Int])
case class ResultDouble(result: Map[String, Double])
case class ResultSummary(result: Seq[Map[String, Double]])

// HUCs have an id and a shape. The shape is GeoJSON, but we've transmitted
// them as Strings in the past so we continue to do so here.
case class HUC (
  id: HucID,
  shape: GeoJSONString // GeoJSON Polygon or MultiPolygon
)

case class Operation (
  name: String, // RasterGroupedCount, RasterGroupedAverage, RasterLinesJoin
  label: OperationID,
  rasters: List[RasterID],
  targetRaster: Option[RasterID],
  pixelIsArea: Option[Boolean]
)

case class MultiInput (
  shapes: List[HUC],
  streamLines: Option[GeoJSONString], // GeoJSON MultiLineString
  operations: List[Operation]
)

object PostRequestProtocol extends DefaultJsonProtocol {
  implicit val inputFormat = jsonFormat10(InputData)
  implicit val postFormat = jsonFormat1(PostRequest)
  implicit val resultFormat = jsonFormat1(ResultInt)
  implicit val resultDoubleFormat = jsonFormat1(ResultDouble)
  implicit val resultSummaryFormat = jsonFormat1(ResultSummary)

  implicit val hucFormat = jsonFormat2(HUC)
  implicit val operationFormat = jsonFormat5(Operation)
  implicit val multiInputFormat = jsonFormat3(MultiInput)
}

object WebServer extends HttpApp with App with LazyLogging with Geoprocessing with ErrorHandler {
  import PostRequestProtocol._

  @throws(classOf[InvalidOperationException])
  def routes: Route =
    handleExceptions(geoprocessingExceptionHandler) {
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
              case "RasterGroupedAverage" =>
                complete(getRasterGroupedAverage(data.input))
              case "RasterLinesJoin" =>
                complete(getRasterLinesJoin(data.input))
              case "RasterSummary" =>
                complete(getRasterSummary(data.input))
              case _ => {
                val message = s"Unknown operationType: ${data.input.operationType}"
                throw new InvalidOperationException(message)
              }
            }
          }
        } ~
        path("multi") {
          entity(as[MultiInput]) { input =>
            complete(getMultiOperations(input))
          }
        }
      }
    }

  val config = ConfigFactory.load()
  val port = config.getInt("geoprocessing.port")
  val host = config.getString("geoprocessing.hostname")

  startServer(host, port)
}
