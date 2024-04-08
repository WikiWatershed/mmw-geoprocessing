package org.wikiwatershed.mmw.geoprocessing

import java.util.function.DoubleBinaryOperator

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import geotrellis.proj4.{CRS, ConusAlbers, LatLng, WebMercator}

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.spark._
import geotrellis.store._
import geotrellis.store.s3._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json.GeoJson

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

    input.polygon.map { str => regularizeMultiPolygon(parseGeom(str)) }
      .unionGeometries
      .asMultiPolygon
      .get
  }

  /**
    * Given input data containing a polygonCRS & a raster CRS, transform a
    * a list of input polygon strings into a sequence of MultiPolygons
    *
    * @param   input  InputData including polygons, polygonCRS, and rasterCRS
    * @return         A Sequence of MultiPolygons
    */
  def createAOIsFromInput(input: InputData): Seq[MultiPolygon] = {
    val parseGeom =
      parseGeometry(_: String, getCRS(input.polygonCRS), getCRS(input.rasterCRS))

    input.polygon.map { str => regularizeMultiPolygon(parseGeom(str)) }
  }

  /**
    * Converts a HUC shape into a reprojected and normalized MultiPolygon
    * Assumes input is in LatLng and rasters are in ConusAlbers
    * @param   huc    HUC containing shape string
    * @return         A MultiPolygon
    */
  def normalizeHuc(huc: HUC): MultiPolygon = {
    regularizeMultiPolygon(parseGeometry(huc.shape, LatLng, ConusAlbers))
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
    GeoJson.parse[Geometry](geoJson) match {
      case p: Polygon => MultiPolygon(p.reproject(srcCRS, destCRS))
      case mp: MultiPolygon => mp.reproject(srcCRS, destCRS)
      case _ => MultiPolygon()
    }
  }

  def regularizeMultiPolygon(mp: MultiPolygon): MultiPolygon = {
    mp.buffer(0) match {
      case p: Polygon if p.isEmpty => MultiPolygon()
      case p: Polygon => MultiPolygon(Seq(p))
      case mp: MultiPolygon => mp
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
  ): Seq[MultiLineString] = {
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
  def parseMultiLineString(geoJson: String, srcCRS: CRS, destCRS: CRS): MultiLineString = {
    GeoJson.parse[Geometry](geoJson) match {
      case l: LineString => MultiLineString(l.reproject(srcCRS, destCRS))
      case ml: MultiLineString => ml.reproject(srcCRS, destCRS)
      case _ => MultiLineString()
    }
  }

  /**
    * Convenience flavor of the above with defaults
    */
  def parseMultiLineString(geoJson: String): MultiLineString =
    parseMultiLineString(geoJson, LatLng, ConusAlbers)

  /**
    * Given a sequence of MultiLines and an area of interest, crops the lines
    * to the area of interest and returns a sequence containing the cropped lines.
    *
    * @param   lines  A sequence of MultiLines
    * @param   aoi    Area of Interest
    * @return         A sequence of MultiLines that intersect with the Area of Interest
    */
  def cropLinesToAOI(lines: Seq[MultiLineString], aoi: MultiPolygon): Seq[MultiLineString] = {
    lines.flatMap(line => (line & aoi).asMultiLineString)
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
    val layout = layers.head.metadata.layout
    lazy val emptyTile = IntConstantTile(NODATA, layout.tileCols, layout.tileRows)
    val maps: Seq[Map[SpatialKey, Tile]] = layers.map((_: Seq[(SpatialKey, Tile)]).toMap)
    val keySet: Array[SpatialKey] = maps.map(_.keySet).reduce(_ union _).toArray
    for (key: SpatialKey <- keySet) yield {
      val tiles: Seq[Tile] = maps.map(_.getOrElse(key, emptyTile))
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

  def printConfiguration() = {
    val config = ConfigFactory.load()

    val host = config.getString("geoprocessing.hostname")
    val port = config.getString("geoprocessing.port")
    val timeout = config.getString("akka.http.server.request-timeout")
    val maxlen = config.getString("akka.http.parsing.max-content-length")

    println("Initializing mmw-geoprocessing with these variables:")
    println(s"MMW_GEOPROCESSING_HOST $host")
    println(s"MMW_GEOPROCESSING_PORT $port")
    println(s"MMW_GEOPROCESSING_BUCKET $s3bucket")
    println(s"MMW_GEOPROCESSING_TIMEOUT $timeout")
    println(s"MMW_GEOPROCESSING_MAXLEN $maxlen")
  }

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
