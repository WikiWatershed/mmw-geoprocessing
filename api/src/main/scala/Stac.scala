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

  def getStacGroupedCount(input: StacInput): Future[Map[String, Int]] = {
    val uri = uri"${input.stacUri}"

    // TODO Calculate this based on where the AoI is
    val targetCRS = ConusAlbers

    val aoi = parseGeometry(input.shape, LatLng, LatLng)
    val reprojectedAoI = aoi.reproject(LatLng, targetCRS)

    val collectionName = StringName(input.stacCollection)
    val searchFilters = SearchFilters(
      collections=List(collectionName.value),
      intersects=Some(aoi),
      datetime=Some(
        TemporalExtent(
          Instant.parse(s"${input.year}-01-02T00:00:00Z"),
          Instant.parse(s"${input.year + 1}-01-01T00:00:00Z")))
    )
    val limit = 100
    val assetName = "supercell".r
    val withGDAL = false
    val parallelMosaicEnable = true

    val backend = AkkaHttpBackend()
    val client = SttpStacClient(backend, uri)

    // Create a Mosaic Raster Source from the STAC Items
    val source = client
      .search(searchFilters)
      .take(limit)
      .compileToFutureList
      .map(MosaicRasterSource.fromStacItems(collectionName, _, assetName, targetCRS, withGDAL, parallelMosaicEnable))

    source
      .nested
      // Clip the Raster Source to the AoI's extent
      .map { rs => rs.read(reprojectedAoI.extent) }
      .value
      .map { _.flatten }
      .map {
        case Some(raster) => raster
          .tile
          // Mask to the AoI
          .mask(reprojectedAoI.extent, reprojectedAoI)
          .band(0)
          .histogram
          .binCounts
          .map { case (value, count) => (s"List(${value})", count.toInt) }
          .toMap
        case None => Map.empty
      }
  }
}
