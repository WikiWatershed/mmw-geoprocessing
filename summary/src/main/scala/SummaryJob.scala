package org.wikiwatershed.mmw.geoprocessing

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector._
import geotrellis.proj4.{ConusAlbers, LatLng, WebMercator}
import geotrellis.raster.rasterize.{Rasterizer, Callback}

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver._

import scala.collection.mutable
import scala.collection.JavaConverters._


/**
  * Convenience class for passing around summary job parameters.
  */
case class SummaryJobParams(nlcdLayerId: LayerId, soilLayerId: LayerId, geometry: Seq[MultiPolygon])

/**
  * The "main" object for this module.
  */
object SummaryJob extends SparkJob with JobUtils {

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
    val getOptional = getOptionalFn(config)

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
    val joinedRasters = nlcd.join(soil)
    val histogramParts = joinedRasters.map { case (key, (nlcdTile, soilTile)) =>
      multiPolygons.map { multiPolygon =>
        val extent = nlcd.metadata.mapTransform(key) // transform spatial key to extent
        val rasterExtent = RasterExtent(extent, nlcdTile.cols, nlcdTile.rows) // transform extent to raster extent
        val clipped = multiPolygon & extent
        val localHistogram = mutable.Map.empty[(Int, Int), Int]

        def intersectionComponentsToHistogram(ps : Seq[Polygon]) = {
          ps.foreach { p =>
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
      (s1 zip s2).map { case (left, right) =>
        (left.toSeq ++ right.toSeq)
          .groupBy(_._1)
          .map { case (nlcdSoilPair, counts) => (nlcdSoilPair, counts.map(_._2).sum) }
      }
    }
  }
}
