package org.wikiwatershed.mmw.geoprocessing

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.collection.mutable

import spark.jobserver._

import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.rasterize.{Rasterizer, Callback}
import geotrellis.spark._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io.json._

case class SummaryJobParams(nlcdLayerId: LayerId, soilLayerId: LayerId, polyMask: Seq[MultiPolygon])

object SummaryJob extends SparkJob {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
    // TODO: Return a more specific failure message
    // Try(parseConfig(config))
    //    .map(_ => SparkJobValid)
    //    .getOrElse(SparkJobInvalid("Failed to parse config parameters. Make sure the request includes input.polymask, input.layer, and input.zoom."))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    def getExtent(geometry: Geometry) : Extent = {
      geometry match {
        case p: Polygon => p.envelope
        case mp: MultiPolygon => mp.envelope
        case gc: GeometryCollection => gc.envelope
      }
    }

    val params = parseConfig(config)
    val extent = getExtent(params.polyMask)
    val nlcdLayer = queryAndCropLayer(catalog(sc), params.nlcdLayerId, extent)
    val soilLayer = queryAndCropLayer(catalog(sc), params.soilLayerId, extent)

    params.polyMask.map {
      mp => histogram(nlcdLayer, soilLayer, mp)
    }
  }

  /*
   * Parse the incoming configuration.
   */
  def parseConfig(config: Config): SummaryJobParams = {
    import scala.collection.JavaConverters._

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
      case _ => WebMercator
    }
    val polyCRS = getOptional("input.polyCRS") match {
      case Some("WebMercator") => WebMercator
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
  def queryAndCropLayer(catalog: S3RasterCatalog, layerId: LayerId, extent: Extent): RasterRDD[SpatialKey] = {
    import geotrellis.spark.op.local._

    layerId match {
      // If the user asked for fake soil data, then some special steps
      // must be taken to synthesize that from nlcd-wm-ext-tms layer.
      case LayerId("soil-fake", zoom) => {
        val newLayerId = LayerId("nlcd-wm-ext-tms", zoom)
        var z : Int = 0
        catalog.query[SpatialKey](newLayerId)
          .where(Intersects(extent))
          .toRDD
          .localMap { y: Int => {
            val a = 8121
            val c = 28411
            val m = 134456
            z = ((a * (z + y) * (z + y) + c) % m)
            (Math.abs(z) % 4) + 1
          }
        }
      }
      // If the user asked for anything else, give them exactly what they asked for.
      case layerId : LayerId => {
        catalog.query[SpatialKey](layerId)
          .where(Intersects(extent))
          .toRDD
      }
    }
  }

  def catalog(sc: SparkContext): S3RasterCatalog = {
    catalog(sc, "com.azavea.datahub", "catalog")
  }

  def catalog(sc: SparkContext, bucket: String, rootPath: String): S3RasterCatalog = {
    S3RasterCatalog(bucket, rootPath)(sc)
  }

  def histogram(nlcd: RasterRDD[SpatialKey], soil: RasterRDD[SpatialKey], mp: MultiPolygon): Map[(Int, Int), Int] = {

    val mapTransform = nlcd.metaData.mapTransform
    val joinedRasters = nlcd.join(soil)
    val histogram = joinedRasters
      .map { case (key, (nlcdTile, soilTile)) =>
        val extent = mapTransform(key) // transform spatial key to map extent
        val rasterExtent = RasterExtent(extent, nlcdTile.cols, nlcdTile.rows) // transform extent to raster extent
        val clipped = mp & extent
        val localHistogram = mutable.Map[(Int, Int), Int]()

        def intersectionComponentsToHistogram(ps : Seq[Polygon]) = {
          ps.map { p =>
            Rasterizer.foreachCellByPolygon(p, rasterExtent)(
              new Callback {
                def apply(col: Int, row: Int): Unit = {
                  val nlcdType = nlcdTile.get(col,row)
                  val soilType = soilTile.get(col,row)
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

    histogram
      .reduce { (m1, m2) =>
      (m1.toSeq ++ m2.toSeq)
        .groupBy(_._1)
        .map { case (key, counts) => (key, counts.map(_._2).sum) }
        .toMap
    }
  }
}
