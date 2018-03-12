package org.wikiwatershed.mmw.geoprocessing

import java.util.function.DoubleBinaryOperator

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import spray.json._

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
    * Given a MultiInput, fetches all rasters specified anywhere
    * in the MultiInput, cropped to a union of all the specified
    * shapes, and maps them to raster names.
    *
    * This map can then be used to look up the relevant raster by
    * an operation at a later time.

    * @param   input  MultiInput including shapes and operations
    * @return         A map from raster names to cropped raster layers
    */
  def collectRastersForInput(
    input: MultiInput,
    shapes: Seq[MultiPolygon]
  ): Map[String, TileLayerCollection[SpatialKey]] = {
    val aoi = shapes.unionGeometries.asMultiPolygon.get
    val ops = input.operations
    val rasterIds = ops.flatMap(_.targetRaster) ++ ops.flatMap(_.rasters)

    rasterIds.distinct.map(r => r -> cropSingleRasterToAOI(r, 0, aoi)).toMap
  }

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
    * Given a MultiInput with a list of shapes, unions all the shapes
    * together into one MultiPolygon, to be used to fetch rasters for
    * all the shapes.
    *
    * @param    input    MultiInput with shapes
    * @return            MultiPolygon
    */
  def createAOIFromInput(input: MultiInput): MultiPolygon =
    input.shapes.map(normalizeHuc)
      .unionGeometries
      .asMultiPolygon
      .get

  /**
    * Converts a HUC shape into a reprojected and normalized MultiPolygon
    * Assumes input is in LatLng and rasters are in ConusAlbers
    * @param   huc    HUC containing shape string
    * @return         A MultiPolygon
    */
  def normalizeHuc(huc: HUC): MultiPolygon = {
    parseGeometry(huc.shape, LatLng, ConusAlbers)
      .buffer(0)
      .asMultiPolygon
      .get
  }

  /**
    * Given an optional boolean value, if it is true, returns Rasterizer
    * Options treating the raster pixels as areas. If false, returns Options
    * treating the raster pixels as points. If not specified, return default
    * value, which treats pixels as points.
    *
    * @param   pixelIsArea  An optional boolean value
    * @return               Rasterizer Options
    */
  def getRasterizerOptions(pixelIsArea: Option[Boolean]): Rasterizer.Options =
    pixelIsArea match {
      case Some(value) =>
        if (value)
          Rasterizer.Options(includePartial = true, sampleType = PixelIsArea)
        else
          Rasterizer.Options(includePartial = true, sampleType = PixelIsPoint)

      case None => Rasterizer.Options.DEFAULT
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
    * Convenience flavor of the above with defaults
    */
  def parseMultiLineString(geoJson: String): MultiLine =
    parseMultiLineString(geoJson, LatLng, ConusAlbers)

  /**
    * Given a sequence of MultiLines and an area of interest, crops the lines
    * to the area of interest and returns a sequence containing the cropped lines.
    *
    * @param   lines  A sequence of MultiLines
    * @param   aoi    Area of Interest
    * @return         A sequence of MultiLines that intersect with the Area of Interest
    */
  def cropLinesToAOI(lines: Seq[MultiLine], aoi: MultiPolygon): Seq[MultiLine] = {
    lines.flatMap(line => (line & aoi).asMultiLine)
  }

  /**
    * For a given config and CRS key, return one of several recognized
    * [[geotrellis.proj4.CRS]]s, or raise an error.
    *
    * @param   crs  The key (e.g. "input.rasterCRS")
    * @return       A CRS
    */
  @throws(classOf[UnknownCRSException])
  def getCRS(crs: String): CRS = crs match {
    case "LatLng" => LatLng
    case "WebMercator" => WebMercator
    case "ConusAlbers" => ConusAlbers
    case s: String => throw new UnknownCRSException(s)
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

  class MinWithoutNoData extends DoubleBinaryOperator {
    override def applyAsDouble(left: Double, right: Double): Double =
      (left, right) match {
        case (`doubleNODATA`, `doubleNODATA`) => Double.MaxValue
        case (`doubleNODATA`, r) => r
        case (l, `doubleNODATA`) => l
        case (l, r) => math.min(l, r)
      }
  }

  class MaxWithoutNoData extends DoubleBinaryOperator {
    override def applyAsDouble(left: Double, right: Double): Double =
      (left, right) match {
        case (`doubleNODATA`, `doubleNODATA`) => Double.MinValue
        case (`doubleNODATA`, r) => r
        case (l, `doubleNODATA`) => l
        case (l, r) => math.max(l, r)
      }
  }
}
