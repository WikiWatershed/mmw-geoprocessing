package org.wikiwatershed.mmw.geoprocessing

import spray.json._
import spray.json.DefaultJsonProtocol._

import geotrellis.proj4.{CRS, ConusAlbers, LatLng, WebMercator}

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._

import com.typesafe.config.ConfigFactory

trait Utils {
  val s3bucket = ConfigFactory.load().getString("geoprocessing.s3bucket")
  val baseLayerReader = S3CollectionLayerReader(s3bucket, "")

  /**
    * Given a zoom level & area of interest, transform a list of raster
    * filenames into a [[TileLayerCollection[SpatialKey]]].
    *
    * @param   rasterIds  A list of raster filenames
    * @param   zoom       The input zoom level
    * @param   aoi        A MultiPolygon area of interest
    * @return             [[TileLayerCollection[SpatialKey]]]
    */
  def cropRastersToAOI(
    rasterIds: List[String],
    zoom: Int,
    aoi: MultiPolygon
  ): Seq[TileLayerCollection[SpatialKey]] =
    rasterIds
      .map { str => LayerId(str, zoom) }
      .map { layer => fetchCroppedLayer(layer, aoi)}

  /**
    * Given input data containing a polygonCRS & a raster CRS, transform an
    * input polygon into a multipolygon AOI
    *
    * @param   input  InputData including polygons, polygonCRS, and rasterCRS
    * @return         A MultiPolygon
    */
  def createAOIFromInput(input: InputData): MultiPolygon =
    input.polygon.map { str =>
      parseGeometry(str, getCRS(input.polygonCRS), getCRS(input.rasterCRS))
        .buffer(0)
        .asMultiPolygon
        .get
    }.unionGeometries.asMultiPolygon.get

  /**
    * Transform the incoming GeoJSON into a [[MultiPolygon]] in the
    * destination CRS.
    *
    * @param   geoJson  The incoming geometry
    * @param   srcCRS   The CRS that the incoming geometry is in
    * @param   destCRS  The CRS that the outgoing geometry should be in
    * @return           A MultiPolygon
    */
  def parseGeometry(geoJson: String, srcCRS: CRS, destCRS: CRS): MultiPolygon = {
    geoJson.parseJson.convertTo[Geometry] match {
      case p: Polygon => MultiPolygon(p.reproject(srcCRS, destCRS))
      case mp: MultiPolygon => mp.reproject(srcCRS, destCRS)
      case _ => MultiPolygon()
    }
  }

  /**
    * For a given config and CRS key, return one of several recognized
    * [[geotrellis.proj4.CRS]]s, or raise an error.
    *
    * @param   crs  The key (e.g. "input.rasterCRS")
    * @return       A CRS
    */
  def getCRS(crs: String): CRS = crs match {
    case "LatLng" => LatLng
    case "WebMercator" => WebMercator
    case "ConusAlbers" => ConusAlbers
    case s: String => throw new Exception(s"Unknown CRS: $s")
  }

  /**
    * From a sequence of layers, return a Map of Tile sequences.
    *
    * @param   layers  A sequence of TileLayerCollections
    * @return          A map of Tile sequences, keyed with the SpatialKey
    */
  def joinCollectionLayers(
    layers: Seq[TileLayerCollection[SpatialKey]]
  ): Map[SpatialKey, Seq[Tile]] = {
    val maps: Seq[Map[SpatialKey, Tile]] = layers.map((_: Seq[(SpatialKey, Tile)]).toMap)
    val keySet: Array[SpatialKey] = maps.map(_.keySet).reduce(_ union _).toArray
    for (key: SpatialKey <- keySet) yield {
      val tiles: Seq[Tile] = maps.map(_.apply(key))
      key -> tiles
    }
  }.toMap

  /**
    * For a layerId and a shape, retrieve an intersecting TileLayerCollection.
    *
    * @param   layerId  The LayerId
    * @param   shape    The shape as a MultiPolygon
    * @return           A TileLayerCollection intersecting the shape
    */
  def fetchCroppedLayer(
    layerId: LayerId,
    shape: MultiPolygon
  ): TileLayerCollection[SpatialKey] =
    baseLayerReader
      .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
      .where(Intersects(shape))
      .result
}
