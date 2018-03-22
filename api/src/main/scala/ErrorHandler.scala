package org.wikiwatershed.mmw.geoprocessing

import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import StatusCodes.{BadRequest,InternalServerError}
import Directives._

sealed trait GeoprocessingException extends Exception
case class MissingTargetRasterException() extends GeoprocessingException()
case class MissingVectorCRSException() extends GeoprocessingException()
case class MissingVectorException() extends GeoprocessingException()
case class MissingStreamLinesException() extends GeoprocessingException()
case class InvalidOperationException(val message: String) extends GeoprocessingException()
case class UnknownCRSException(val crs: String) extends GeoprocessingException()

trait ErrorHandler {
  val geoprocessingExceptionHandler = ExceptionHandler {
    case InvalidOperationException(e) => {
      println(s"Invalid operation type: $e")
      complete(HttpResponse(BadRequest, entity = e))
    }
    case MissingTargetRasterException() => {
      println("Input error: Missing required targetRaster")
      complete(HttpResponse(BadRequest, entity = "Missing required targetRaster"))
    }
    case MissingVectorCRSException() => {
      println("Input error: Missing required vectorCRS")
      complete(HttpResponse(BadRequest, entity = "Missing required vectorCRS"))
    }
    case MissingVectorException() => {
      println(s"Input error: Missing required vector")
      complete(HttpResponse(BadRequest, entity = "Missing required vector"))
    }
    case MissingStreamLinesException() => {
      println(s"Input error: Missing required streamLines")
      complete(HttpResponse(BadRequest, entity = "Missing required streamLines"))
    }
    case UnknownCRSException(crs) => {
      println(s"Unknown CRS error: $crs")
      complete(HttpResponse(BadRequest, entity = "Unknown CRS"))
    }
    case e: Exception => {
      println(s"Exception: $e")
      complete(HttpResponse(InternalServerError, entity = s"$e"))
    }
  }
}
