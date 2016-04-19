package org.wikiwatershed.mmw.geoprocessing

import geotrellis.spark.{LayerId, SpatialKey, TileLayerRDD}
import geotrellis.vector.{GeometryCollection, MultiPolygon, MultiPolygonResult, Polygon, PolygonResult}
import com.typesafe.config.Config
import geotrellis.raster.rasterize.{Callback, Rasterizer}
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}


trait MapshedJobParams

case class RasterVectorJobParams(
  polygon: Seq[MultiPolygon],
  vector: GeometryCollection,
  rasterLayerId: LayerId
) extends MapshedJobParams

object MapshedJob extends SparkJob with JobUtils {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    // TODO Add real validation
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
     parseConfig(config) match {
      case RasterVectorJobParams(polygon, vector, rasterLayerId) =>
        val extent = GeometryCollection(polygon).envelope
        val rasterLayer = queryAndCropLayer(catalog(sc), rasterLayerId, extent)
         // rasterVectorJoin(vector, rasterLayer)
         println(s"XXX $vector")
      case _ => throw new Exception("Unknown Job Type")
    }
  }

  def parseConfig(config: Config): MapshedJobParams = {
    import scala.collection.JavaConverters._
    import geotrellis.proj4.{LatLng, WebMercator}

    val getOptional = getOptionalFn(config)

    config.getString("input.operationType") match {
      case "RasterVectorJoin" =>
        val rasterCRS = getOptional("input.rasterCRS") match {
          case Some("LatLng") => LatLng
          case Some("WebMercator") => WebMercator
          case Some("ConsuAlbers") => ConusAlbers
          case _ => ConusAlbers
        }
        val polygonCRS = getOptional("input.polygonCRS") match {
          case Some("LatLng") => LatLng
          case Some("WebMercator") => WebMercator
          case Some("ConusAlbers") => ConusAlbers
          case _ => LatLng
        }
        val vectorCRS = getOptional("input.vectorCRS") match {
          case Some("LatLng") => LatLng
          case Some("WebMercator") => WebMercator
          case Some("ConusAlbers") => ConusAlbers
          case _ => LatLng
        }
        val polygon = config.getStringList("input.polygon").asScala.map {
          str => parseGeometry(str, polygonCRS, rasterCRS)
        }
        val vector = config.getStringList("input.vector").asScala.map {
          str => parseGeometry(str, vectorCRS, rasterCRS)
        }
        val zoom = config.getInt("input.zoom")
        val rasterLayerId = LayerId(config.getString("input.raster"), zoom)

        RasterVectorJobParams(polygon, vector, rasterLayerId)

      case _ => throw new Exception("Unknown Job Type")
    }
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
