package org.wikiwatershed.mmw.geoprocessing

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.syntax.functor._
import cats.syntax.nested._
import cats.syntax.option._
import com.azavea.stac4s._
import com.azavea.stac4s.api.client._
import eu.timepit.refined.types.all.NonEmptyString
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.effects.MosaicRasterSourceIO
import geotrellis.stac.raster._
import geotrellis.vector._
import sttp.client3.UriContext
import sttp.client3.akkahttp._

import java.time.Instant
import scala.concurrent._

trait Stac extends Utils {
  import scala.concurrent.ExecutionContext.Implicits.global

  def getStacInfo(shape: SimpleShape): Future[String] = {
    val uri = uri"https://api.impactobservatory.com/stac-aws"

    val aoi = parseGeometry(shape.shape, LatLng, LatLng)

//    val searchFilters = SearchFilters(bbox=Some(bbox))
    val collectionName = StringName("io-10m-annual-lulc")
    val searchFilters = SearchFilters(
      collections=List(collectionName.value),
      intersects=Some(aoi),
//      query=Map("start_datetime" -> List(GreaterThanEqual(JString("2023-01-01T00:00:00Z"))))
//      query=Map(
//        "start_datetime" -> GreaterThanEqual(Json.withString("2023-01-01T00:00:00Z")),
//        "end_datetime" -> LessThanEqual(Json.withString("2024-01-01T00:00:00Z")),
//      )
//      datetime="2023-01-01T00:00:00Z/2023-12-31T23:59:59Z"
      datetime=Some(
        TemporalExtent(
          Instant.parse("2023-01-02T00:00:00Z"),
          Instant.parse("2024-01-01T00:00:00Z")))
    )
    val limit = 100
    val assetName = "supercell".r
    val withGDAL = false
    val defaultCRS = WebMercator
    val parallelMosaicEnable = false
//    val extent = Extent(-180, -90, 180, 90)
//    val searchFilters = SearchFilters(bbox=Some(TwoDimBbox(extent.xmin, extent.ymin, extent.xmax, extent.ymax)))

    val backend = AkkaHttpBackend()
    val client = SttpStacClient(backend, uri)

    val source = client
      .search(searchFilters)
      .take(limit)
      .compileToFutureList
      .map(MosaicRasterSource.fromStacItems(collectionName, _, assetName, defaultCRS, withGDAL, parallelMosaicEnable))

//    source.map {
//      _.map(_.id).mkString(",")
//    }

//    futureInt

//    val tiffCRS: Future[CRS] = source.map { _.map { _.crs } getOrElse WebMercator }

//    source
//      .map {
//        _.map { _.crs.toString } mkString "," }

    source
      .nested
      .map { rs => rs.read(aoi.reproject(LatLng, rs.crs).extent) }
      .value
      .map { _.flatten }
      .map { _.map { _.tile.size } mkString "," }

//    val result = source
//      .nested
//      .map(_.read(aoi.extent))
//      .value
//      .map(_.flatten)
//      .map {
//        case Some(raster) => s"raster.extent = ${raster.extent}"
//        case None => s"no rasters found in extent ${aoi.extent}"
//      }
//
//    result
  }
}
