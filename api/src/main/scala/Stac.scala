package org.wikiwatershed.mmw.geoprocessing

import cats.syntax.functor._
import cats.syntax.nested._
import com.azavea.stac4s._
import com.azavea.stac4s.api.client._
import geotrellis.proj4._
import geotrellis.raster._
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

    val collectionName = StringName("io-10m-annual-lulc")
    val searchFilters = SearchFilters(
      collections=List(collectionName.value),
      intersects=Some(aoi),
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

    val backend = AkkaHttpBackend()
    val client = SttpStacClient(backend, uri)

    val source = client
      .search(searchFilters)
      .take(limit)
      .compileToFutureList
      .map(MosaicRasterSource.fromStacItems(collectionName, _, assetName, defaultCRS, withGDAL, parallelMosaicEnable))

    source
      .nested
      .map { rs => rs.read(aoi.reproject(LatLng, rs.crs).extent) }
      .value
      .map { _.flatten }
      .map { _.map { _.tile.size } mkString "," }
  }
}
