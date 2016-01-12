package org.wikiwatershed.mmw.geoprocessing

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.SparkContext._

import spark.jobserver._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.json._
import geotrellis.vector._

case class SummaryJobParams(nlcdLayerId: LayerId, soilLayerId: LayerId, geometry: Seq[MultiPolygon])

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

  /*
   * Parse the incoming configuration.
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

  /*
   * Transform the incoming GeoJSON into a sequence of MultiPolygons
   * in the destination CRS.
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

  /*
   * Fetch a particular layer from the catalogue, restricted to the
   * given extent, and return a RasterRDD of the result.
   */
  def queryAndCropLayer(catalog : S3LayerReader[SpatialKey, Tile, RasterMetaData], layerId: LayerId, extent: Extent): RasterRDD[SpatialKey] = {
    import geotrellis.spark.op.local._
    import geotrellis.spark.io.Intersects

      layerId match {
      case layerId : LayerId => {
        catalog.query(layerId)
          .where(Intersects(extent))
          .toRDD
      }
    }
  }

  def catalog(sc: SparkContext): S3LayerReader[SpatialKey, Tile, RasterMetaData] =
    catalog("azavea-datahub", "catalog")(sc)

  def catalog(bucket: String, rootPath: String)(implicit sc: SparkContext): S3LayerReader[SpatialKey, Tile, RasterMetaData] = {
    val attributeStore = new S3AttributeStore("azavea-datahub", "catalog")
    val rddReader = new S3RDDReader[SpatialKey, Tile]()
    val catalog = new S3LayerReader[SpatialKey, Tile, RasterMetaData](attributeStore, rddReader, None)
    catalog
  }

  def histograms(nlcd: RasterRDD[SpatialKey], soil: RasterRDD[SpatialKey], mps: Seq[MultiPolygon]): Seq[Map[(Int, Int), Int]] = {
    import scala.collection.mutable
    import geotrellis.raster.rasterize.{Rasterizer, Callback}

    val mapTransform = nlcd.metaData.mapTransform
    val joinedRasters = nlcd.join(soil)
    val histogramParts = joinedRasters.map { case (key, (nlcdTile, soilTile)) =>
      mps.map { mp =>
        val extent = mapTransform(key) // transform spatial key to extent
        val rasterExtent = RasterExtent(extent, nlcdTile.cols, nlcdTile.rows) // transform extent to raster extent
        val clipped = mp & extent
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
          case PolygonResult(p) => intersectionComponentsToHistogram(List(p))
          case MultiPolygonResult(mp) => intersectionComponentsToHistogram(mp.polygons)
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
