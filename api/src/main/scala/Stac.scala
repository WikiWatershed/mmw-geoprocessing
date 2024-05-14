package org.wikiwatershed.mmw.geoprocessing

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.syntax.functor._
import cats.syntax.nested._
import cats.syntax.option._
import com.azavea.stac4s.{Bbox, TwoDimBbox}
import com.azavea.stac4s.api.client.{SearchFilters, SttpStacClient}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.effects.MosaicRasterSourceIO
import geotrellis.raster.{MosaicRasterSource, StringName}
import geotrellis.stac.raster.{StacAssetRasterSource, StacItemAsset}
import geotrellis.vector.Extent
import sttp.client3.UriContext
import sttp.client3.akkahttp._

import scala.concurrent.{Await, Future}
import geotrellis.vector.MultiPolygon

trait Stac extends Utils {
  import scala.concurrent.ExecutionContext.Implicits.global

  def getStacInfo(shape: SimpleShape): Future[String] = {
    val uri = uri"https://api.impactobservatory.com/stac-aws"

    val aoi = parseGeometry(shape.shape, LatLng, LatLng)
    val extent = Extent(aoi.getEnvelopeInternal())
    val bbox = TwoDimBbox(extent.xmin, extent.ymin, extent.xmax, extent.ymax)

    val searchFilters = SearchFilters(bbox=Some(bbox))
//    val searchFilters = SearchFilters()
    val limit = 100
    val assetName = "supercell".r
    val withGDAL = false
    val defaultCRS = WebMercator
    val parallelMosaicEnable = false
    val collectionName = StringName("io-10m-annual-lulc")
//    val extent = Extent(0, 0, 180, 180)

    val backend = AkkaHttpBackend()
    val client = SttpStacClient(backend, uri)

    val source = client
      .search(searchFilters)
      .take(limit)
      .compileToFutureList
      .map(MosaicRasterSource.fromStacItems(collectionName, _, assetName, defaultCRS, withGDAL, parallelMosaicEnable))

    val result = source.nested.map(_.read(extent)).value.map(_.flatten).map {
      case Some(raster) => s"raster.extent = ${raster.extent}"
      case None => s"no rasters found in extent ${extent}"
    }

    result
  }
}
