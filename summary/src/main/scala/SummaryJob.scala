package org.wikiwatershed.mmw.geoprocessing

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io._

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.SparkContext._
import spark.jobserver._


/**
  * Convenience class for passing around summary job parameters.
  */
case class SummaryJobParams(nlcdLayerId: LayerId, soilLayerId: LayerId, geometry: Seq[MultiPolygon])

/**
  * The "main" object for this module.
  */
object SummaryJob extends SparkJob {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
    // TODO: Return a more specific failure message
    // Try(parseConfig(config))
    //    .map(_ => SparkJobValid)
    //    .getOrElse(SparkJobInvalid("Failed to parse config parameters. Make sure the request includes input.polymask, input.layer, and input.zoom."))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val params = parseConfig(config)
    val extent = GeometryCollection(params.geometry).envelope
    val nlcdLayer = queryAndCropLayer(catalog(sc), params.nlcdLayerId, extent)
    val soilLayer = queryAndCropLayer(catalog(sc), params.soilLayerId, extent)

    histograms(nlcdLayer, soilLayer, params.geometry)
  }

  /**
    * Parse the incoming configuration.
    *
    * @param  config  The configuration to parse
    * @return         The NLCD layer, SOIL layer, and query polygon
    */
  def parseConfig(config: Config): SummaryJobParams = {
    import scala.collection.JavaConverters._
    import geotrellis.proj4._

    def getOptional(key : String) : Option[String] = {
      config.hasPath(key) match {
        case true => Option(config.getString(key))
        case false => None
      }
    }

    val zoom = config.getInt("input.zoom")
    val nlcdLayer = LayerId(config.getString("input.nlcdLayer"), zoom)
    val soilLayer = LayerId(config.getString("input.soilLayer"), zoom)
    val tileCRS = getOptional("input.tileCRS") match {
      case Some("LatLng") => LatLng
      case Some("WebMercator") => WebMercator
      case Some("ConusAlbers") => ConusAlbers
      case _ => ConusAlbers
    }
    val polyCRS = getOptional("input.polyCRS") match {
      case Some("LatLng") => LatLng
      case Some("WebMercator") => WebMercator
      case Some("ConusAlbers") => ConusAlbers
      case _ => LatLng
    }
    val geometry = config.getStringList("input.geometry").asScala.map {
      str => parseGeometry(str, polyCRS, tileCRS)
    }

    SummaryJobParams(nlcdLayer, soilLayer, geometry)
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
  def parseGeometry(geoJson: String, srcCRS: geotrellis.proj4.CRS, destCRS: geotrellis.proj4.CRS) : MultiPolygon = {
    import spray.json._
    import geotrellis.vector.io.json._
    import geotrellis.vector.reproject._

    geoJson.parseJson.convertTo[Geometry] match {
      case p: Polygon => MultiPolygon(p.reproject(srcCRS, destCRS))
      case mp: MultiPolygon => mp.reproject(srcCRS, destCRS)
      case _ => MultiPolygon()
    }
  }

  /**
    * Fetch a particular layer from the catalog, restricted to the
    * given extent, and return a [[TileLayerRDD]] of the result.
    *
    * @param   catalog  The S3 location from which the data should be read
    * @param   layerId  The layer that should be read
    * @param   extent   The extent (subset) of the layer that should be read
    * @return           An RDD of [[SpatialKey]]s
    */
  def queryAndCropLayer(catalog : S3LayerReader, layerId: LayerId, extent: Extent): TileLayerRDD[SpatialKey] = {
    import geotrellis.spark.mapalgebra.local._

      layerId match {
      case layerId : LayerId => {
        catalog.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
          .where(Intersects(extent))
          .result
      }
    }
  }

  /**
    * Return an [[S3LayerReader]] object to read from the catalog
    * directory in the azavea datahub.
    *
    * @return  An S3LayerReader object
    */
  def catalog(sc: SparkContext): S3LayerReader =
    catalog("azavea-datahub", "catalog")(sc)

  /**
    * Take a bucket and a catalog, and return an [[S3LayerReader]]
    * object to read from it.
    *
    * @param   bucket   The name of the S3 bucket
    * @param   catalog  The name of the catalog (child of the root directory) in the bucket
    * @return           An S3LayerReader object
    */
  def catalog(bucket: String, rootPath: String)(implicit sc: SparkContext): S3LayerReader = {
    val attributeStore = new S3AttributeStore(bucket, rootPath)
    val catalog = new S3LayerReader(attributeStore)

    catalog
  }

  /**
    * Compute the histogram of the intersection of a pair of layers
    * with the query polygon list, and report it as a list of maps
    * from (Int, Int) to Int.
    *
    * By "pair of layers" what we mean is that the two layers are
    * essentially overlayed, and the histogram is taken over pairs of
    * values (where one value comes from the first layer, and the
    * other value from the second layer).
    *
    * @param   nlcd           The first layer
    * @param   soil           The second layer
    * @param   multiPolygons  The query polygons
    * @return                 The histograms for the respective query polygons
    */
  def histograms(nlcd: TileLayerRDD[SpatialKey], soil: TileLayerRDD[SpatialKey], multiPolygons: Seq[MultiPolygon]): Seq[Map[(Int, Int), Int]] = {
    import scala.collection.mutable
    import geotrellis.raster.rasterize.{Rasterizer, Callback}

    val mapTransform = nlcd.metadata.mapTransform
    val joinedRasters = nlcd.join(soil)
    val histogramParts = joinedRasters.map { case (key, (nlcdTile, soilTile)) =>
      multiPolygons.map { multiPolygon =>
        val extent = mapTransform(key) // transform spatial key to extent
        val rasterExtent = RasterExtent(extent, nlcdTile.cols, nlcdTile.rows) // transform extent to raster extent
        val clipped = multiPolygon & extent
        val localHistogram = mutable.Map.empty[(Int, Int), Int]

        def intersectionComponentsToHistogram(ps : Seq[Polygon]) = {
          ps.map { p =>
            Rasterizer.foreachCellByPolygon(p, rasterExtent)(
              new Callback {
                def apply(col: Int, row: Int): Unit = {
                  val nlcdType = nlcdTile.get(col,row)
                  val soilType = soilTile.get(col,row) match {
                    case NODATA => 3
                    case n : Int => n
                  }
                  val pair = (nlcdType, soilType)
                  if(!localHistogram.contains(pair)) { localHistogram(pair) = 0 }
                  localHistogram(pair) += 1
                }
              }
            )
          }
        }

        clipped match {
          case PolygonResult(polygon) => intersectionComponentsToHistogram(List(polygon))
          case MultiPolygonResult(multiPolygon) => intersectionComponentsToHistogram(multiPolygon.polygons)
          case _ =>
        }

        localHistogram.toMap
      }
    }

    histogramParts.reduce { (s1, s2) =>
      (s1 zip s2).map { t =>
        (t._1.toSeq ++ t._2.toSeq)
          .groupBy(_._1)
          .map { case (key, counts) => (key, counts.map(_._2).sum) }
          .toMap
      }
    }
  }
}
