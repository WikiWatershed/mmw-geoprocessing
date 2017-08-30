package org.wikiwatershed.mmw.geoprocessing

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

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
  ): Future[Seq[TileLayerCollection[SpatialKey]]] =
    Future.sequence {
      rasterIds.map { str => Future(cropSingleRasterToAOI(str, zoom, aoi))}
    }

  /**
    * Given a zoom level & area of interest, transform a raster filename into a
    * TileLayerCollection[SpatialKey].
    *
    * @param   rasterId   The raster filename
    * @param   zoom       The input zoom level
    * @param   aoi        A MultiPolygon area of interest
    * @return             TileLayerCollection[SpatialKey]
    */
  def cropSingleRasterToAOI(
    rasterId: String,
    zoom: Int,
    aoi: MultiPolygon
  ): TileLayerCollection[SpatialKey] =
    fetchCroppedLayer(LayerId(rasterId, zoom), aoi)

  /**
    * Given input data containing a polygonCRS & a raster CRS, transform an
    * input polygon into a multipolygon AOI
    *
    * @param   input  InputData including polygons, polygonCRS, and rasterCRS
    * @return         A MultiPolygon
    */
  def createAOIFromInput(input: InputData): MultiPolygon = {
    val parseGeom =
      parseGeometry(_: String, getCRS(input.polygonCRS), getCRS(input.rasterCRS))

    input.polygon.map { str => parseGeom(str).buffer(0).asMultiPolygon.get }
      .unionGeometries
      .asMultiPolygon
      .get
  }

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
    * Given an input vector along with a vectorCRS and rasterCRS, return a Seq
    * of MultiLines
    *
    * @param   vector     A list of strings representing the input vector
    * @param   vectorCRS  CRS for the input vector
    * @param   rasterCRS  CRS for the input raster IDs
    * @return             A list of MultiLines
    */
  def createMultiLineFromInput(
    vector: List[String],
    vectorCRS: String,
    rasterCRS: String
  ): Seq[MultiLine] = {
    val parseVector =
      parseMultiLineString(_: String, getCRS(vectorCRS), getCRS(rasterCRS))

    vector.map { str => parseVector(str) }
  }

  /**
    * Transform the incoming stream vector into a MultiLine in the
    * destination CRS.
    *
    * @param   geoJson  The incoming geometry
    * @param   srcCRS   The CRS that the incoming geometry is in
    * @param   destCRS  The CRS that the outgoing geometry should be in
    * @return           A MultiLine
    */
  def parseMultiLineString(geoJson: String, srcCRS: CRS, destCRS: CRS): MultiLine = {
    geoJson.parseJson.convertTo[Geometry] match {
      case l: Line => MultiLine(l.reproject(srcCRS, destCRS))
      case ml: MultiLine => ml.reproject(srcCRS, destCRS)
      case _ => MultiLine()
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
