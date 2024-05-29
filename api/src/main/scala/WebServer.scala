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
case class ResultManyInt(result: Seq[Map[String, Int]])

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
  streamLines: Option[List[GeoJSONString]], // GeoJSON MultiLineString
  operations: List[Operation]
)

case class StacInput (
  shape: GeoJSONString,
  stacUri: String,
  stacCollection: String,
  year: Int,
)

object PostRequestProtocol extends DefaultJsonProtocol {
  implicit val inputFormat: RootJsonFormat[InputData] = jsonFormat10(InputData)
  implicit val postFormat: RootJsonFormat[PostRequest] = jsonFormat1(PostRequest)
  implicit val resultFormat: RootJsonFormat[ResultInt] = jsonFormat1(ResultInt)
  implicit val resultDoubleFormat: RootJsonFormat[ResultDouble] = jsonFormat1(ResultDouble)
  implicit val resultSummaryFormat: RootJsonFormat[ResultSummary] = jsonFormat1(ResultSummary)
  implicit val resultManyIntFormat: RootJsonFormat[ResultManyInt] = jsonFormat1(ResultManyInt)

  implicit val hucFormat: RootJsonFormat[HUC] = jsonFormat2(HUC)
  implicit val operationFormat: RootJsonFormat[Operation] = jsonFormat5(Operation)
  implicit val multiInputFormat: RootJsonFormat[MultiInput] = jsonFormat3(MultiInput)

  implicit val stacInputFormat: RootJsonFormat[StacInput] = jsonFormat4(StacInput)
}

object WebServer extends HttpApp with App with LazyLogging with Geoprocessing with Stac with ErrorHandler {
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
              case "RasterGroupedCountMany" =>
                complete(getRasterGroupedCountMany(data.input))
              case "RasterGroupedAverage" =>
                complete(getRasterGroupedAverage(data.input))
              case "RasterLinesJoin" =>
                complete(getRasterLinesJoin(data.input))
              case "RasterSummary" =>
                complete(getRasterSummary(data.input))
              case _ => {
                val message = s"Unknown operationType: ${data.input.operationType}"
                throw InvalidOperationException(message)
              }
            }
          }
        } ~
        path("multi") {
          entity(as[MultiInput]) { input =>
            complete(getMultiOperations(input))
          }
        } ~
        path("stac") {
          entity(as[StacInput]) { input =>
            complete(getStacGroupedCount(input))
          }
        }
      }
    }

  val config = ConfigFactory.load()
  val port = config.getInt("geoprocessing.port")
  val host = config.getString("geoprocessing.hostname")

  printConfiguration()

  startServer(host, port)
}
