package org.wikiwatershed.mmw.geoprocessing

import geotrellis.raster._
import geotrellis.raster.rasterize.{Callback, Rasterizer}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerRDD}
import geotrellis.vector._
import geotrellis.vector.io._

import com.typesafe.config.Config
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.mutable


sealed trait MapshedJobParams

case class RasterLinesJobParams(
  polygon: Seq[MultiPolygon],
  lines: Seq[Line],
  rasterLayerId: LayerId
) extends MapshedJobParams

object MapshedJob extends SparkJob with JobUtils {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    // TODO Add real validation
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
     parseConfig(config) match {
      case RasterLinesJobParams(polygon, lines, rasterLayerId) =>
        val extent = GeometryCollection(polygon).envelope
        val rasterLayer = queryAndCropLayer(catalog(sc), rasterLayerId, extent)

        rasterLinesJoin(rasterLayer, lines)

      case _ =>
         throw new Exception("Unknown Job Type")
    }
  }

  def parseConfig(config: Config): MapshedJobParams = {
    val crs: String => geotrellis.proj4.CRS = getCRS(config, _)

    config.getString("input.operationType") match {
      case "RasterLinesJoin" =>
        val rasterCRS = crs("input.rasterCRS")
        val polygonCRS = crs("input.polygonCRS")
        val linesCRS = crs("input.vectorCRS")
        val polygon = config.getStringList("input.polygon").asScala.map({ str => parseGeometry(str, polygonCRS, rasterCRS) })
        val lines = config.getStringList("input.vector").asScala.map({ str => str.parseJson.convertTo[Line].reproject(linesCRS, rasterCRS) })
        val zoom = config.getInt("input.zoom")
        val rasterLayerId = LayerId(config.getString("input.raster"), zoom)

        RasterLinesJobParams(polygon, lines, rasterLayerId)

      case _ =>
        throw new Exception("Unknown Job Type")
    }
  }

  def rasterLinesJoin(rasterLayer: TileLayerRDD[SpatialKey], lines: Seq[Line]): Map[Int, Int] = {
    val rtree = new STRtree

    lines.foreach({lineString =>
      val Extent(xmin, ymin, xmax, ymax) = lineString.envelope
      rtree.insert(new Envelope(xmin, xmax, ymin, ymax), lineString)
    })

    rasterLayer
      .map({ case (key, tile) => {
        val extent = rasterLayer.metadata.mapTransform(key)
        val Extent(xmin, ymin, xmax, ymax) = extent
        val rasterExtent = RasterExtent(extent, tile.cols, tile.rows)
        val pixels = mutable.ListBuffer.empty[(Int, Int)]
        val cb = new Callback {
          def apply(col: Int, row: Int): Unit = {
            val pixel = (col, row)
            pixels += pixel
          }
        }

        rtree.query(new Envelope(xmin, xmax, ymin, ymax)).asScala.foreach({ lineStringObject =>
          Rasterizer.foreachCellByLineString(lineStringObject.asInstanceOf[Line], rasterExtent)(cb)
        })

        pixels
          .distinct.map({ case (col, row) => tile.get(col, row) })
          .groupBy(identity).map({ case (k: Int, list: mutable.ListBuffer[Int]) => (k -> list.length) })
          .toList
      }})
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k: Int, list: List[(Int, Int)]) => (k -> list.map(_._2).sum) })
  }
}
