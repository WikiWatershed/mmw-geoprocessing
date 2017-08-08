package org.wikiwatershed.mmw.geoprocessing

import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.http.scaladsl.server.{ HttpApp, Route }
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import geotrellis.raster.NODATA

object WebServer extends HttpApp with App with LazyLogging {
  def routes: Route =
    get {
      path("ping") {
        complete("pong")
      }
    } ~
    post {
      path("run") {
        entity(as[String]) { input =>
          println(input)
          val output = Map(
            List(90, 1) -> 1,
            List(31, 3) -> 57,
            List(81, 7) -> 514,
            List(52, 7) -> 416,
            List(43, 7) -> 36,
            List(21, 3) -> 8134,
            List(43, 2) -> 46,
            List(23, 2) -> 670,
            List(24, 1) -> 62,
            List(23, 1) -> 512,
            List(23, 7) -> 311,
            List(41, 1) -> 50,
            List(21, 6) -> 176,
            List(21, NODATA) -> 16027,
            List(81, 3) -> 2647,
            List(31, 2) -> 21,
            List(71, 4) -> 126,
            List(42, 3) -> 72,
            List(52, 1) -> 13,
            List(43, 4) -> 50,
            List(31, 4) -> 165,
            List(71, NODATA) -> 72,
            List(22, 7) -> 969,
            List(22, NODATA) -> 16279,
            List(31, 7) -> 58,
            List(24, 7) -> 33,
            List(22, 1) -> 603,
            List(81, 6) -> 10,
            List(82, 4) -> 2133,
            List(41, 4) -> 5379,
            List(82, NODATA) -> 268,
            List(22, 2) -> 1636,
            List(21, 1) -> 1601,
            List(81, 2) -> 1956,
            List(90, 6) -> 19,
            List(41, 2) -> 3008,
            List(41, 7) -> 4232,
            List(81, 1) -> 28,
            List(95, 3) -> 14,
            List(23, 6) -> 10,
            List(82, 3) -> 1889,
            List(42, 2) -> 13,
            List(21, 4) -> 6982,
            List(43, NODATA) -> 106,
            List(52, 4) -> 971,
            List(82, 7) -> 306,
            List(90, 4) -> 509,
            List(95, 4) -> 27,
            List(21, 7) -> 3241,
            List(81, NODATA) -> 1086,
            List(52, NODATA) -> 585,
            List(71, 6) -> 7,
            List(11, 1) -> 2,
            List(71, 2) -> 157,
            List(90, NODATA) -> 399,
            List(11, NODATA) -> 32,
            List(41, 3) -> 4419,
            List(24, 3) -> 372,
            List(42, 4) -> 43,
            List(11, 4) -> 5,
            List(95, 7) -> 20,
            List(22, 4) -> 2876,
            List(90, 7) -> 2500,
            List(24, 4) -> 100,
            List(41, NODATA) -> 2068,
            List(82, 2) -> 1716,
            List(52, 3) -> 960,
            List(42, NODATA) -> 25,
            List(95, 2) -> 2,
            List(90, 3) -> 404,
            List(52, 2) -> 357,
            List(22, 6) -> 47,
            List(31, NODATA) -> 63,
            List(95, NODATA) -> 49,
            List(23, 3) -> 1188,
            List(23, NODATA) -> 7223,
            List(41, 6) -> 62,
            List(24, NODATA) -> 3148,
            List(24, 2) -> 78,
            List(21, 2) -> 4397,
            List(22, 3) -> 2820,
            List(52, 6) -> 7,
            List(90, 2) -> 108,
            List(43, 3) -> 91,
            List(71, 7) -> 101,
            List(81, 4) -> 2681,
            List(71, 3) -> 221,
            List(23, 4) -> 1062,
            List(82, 1) -> 33
          ) map { case (k, v) => k.toString -> v}
          complete(output.toJson)
        }
      }
    }

  val config = ConfigFactory.load()
  val port = config.getInt("geoprocessing.port")
  val host = config.getString("geoprocessing.hostname")

  startServer(host, port)
}
