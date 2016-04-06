package org.wikiwatershed.mmw.geoprocessing

import geotrellis.spark.{LayerId, SpatialKey, TileLayerRDD}
import geotrellis.vector.{GeometryCollection, MultiPolygon, MultiPolygonResult, Polygon, PolygonResult}
import com.typesafe.config.Config
import geotrellis.raster.rasterize.{Callback, Rasterizer}
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}


case class MapshedJobParams(nlcdLayerId: LayerId, geometry: Seq[MultiPolygon])

object MapshedJob extends SparkJob with JobUtils {
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    // TODO Add real validation
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val params = parseConfig(config)
    val extent = GeometryCollection(params.geometry).envelope
    val nlcdLayer = queryAndCropLayer(catalog(sc), params.nlcdLayerId, extent)

    histogram(nlcdLayer, params.geometry)
  }

  def parseConfig(config: Config): MapshedJobParams = {
    import scala.collection.JavaConverters._
    import geotrellis.proj4.{LatLng, WebMercator}

    val getOptional = getOptionalFn(config)

    val zoom = config.getInt("input.zoom")
    val nlcdLayer = LayerId(config.getString("input.nlcdLayer"), zoom)

    val tileCRS = getOptional("input.tileCRS") match {
      case Some("LatLng") => LatLng
      case Some("WebMercator") => WebMercator
      case Some("ConsuAlbers") => ConusAlbers
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

    MapshedJobParams(nlcdLayer, geometry)
  }

  def histogram(layer: TileLayerRDD[SpatialKey], multiPolygons: Seq[MultiPolygon]) = {
    import scala.collection.mutable
    import geotrellis.raster.RasterExtent

    val histogramParts = layer.map { case (key, tile) =>
      multiPolygons.map { multiPolygon =>
        val extent = layer.metadata.mapTransform(key) // transform spatial key to extent
        val rasterExtent = RasterExtent(extent, tile.cols, tile.rows) // transform extent to raster extent
        val clipped = multiPolygon & extent
        val localHistogram = mutable.Map.empty[Int, Int]

        def intersectionComponentsToHistogram(ps: Seq[Polygon]) = {
          ps.foreach { p =>
            Rasterizer.foreachCellByPolygon(p, rasterExtent)(
              new Callback {
                def apply(col: Int, row: Int): Unit = {
                  val nlcdType = tile.get(col, row)

                  if (!localHistogram.contains(nlcdType)) {
                    localHistogram(nlcdType) = 0
                  }

                  localHistogram(nlcdType) += 1
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
      (s1 zip s2).map { case (left, right) =>
        (left.toSeq ++ right.toSeq)
          .groupBy(_._1)
          .map { case (nlcdType, counts) =>
            (nlcdType, counts.map(_._2).sum)
          }
      }
    }
  }
}
