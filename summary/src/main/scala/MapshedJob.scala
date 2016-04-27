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
  lines: Seq[MultiLine],
  rasterLayerIds: Seq[LayerId]
) extends MapshedJobParams

object MapshedJob extends SparkJob with JobUtils {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    // TODO Add real validation
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
     parseConfig(config) match {
      case RasterLinesJobParams(polygon, lines, rasterLayerIds) =>
        val extent = GeometryCollection(polygon).envelope
        val rasterLayers = rasterLayerIds.map({ rasterLayerId =>
          queryAndCropLayer(catalog(sc), rasterLayerId, extent)
        })

        rasterLinesJoin(rasterLayers, lines)

      case _ =>
         throw new Exception("Unknown Job Type")
    }
  }

  def parseConfig(config: Config): MapshedJobParams = {
    val crs: String => geotrellis.proj4.CRS = getCRS(config, _)

    config.getString("input.operationType") match {
      case "RasterLinesJoin" =>
        val zoom = config.getInt("input.zoom")

        val rasterCRS = crs("input.rasterCRS")
        val polygonCRS = crs("input.polygonCRS")
        val linesCRS = crs("input.vectorCRS")

        val rasterLayerIds = config.getStringList("input.rasters").asScala.map({ str => LayerId(str, zoom) })
        val polygon = config.getStringList("input.polygon").asScala.map({ str => parseGeometry(str, polygonCRS, rasterCRS) })
        val lines = config.getStringList("input.vector").asScala.map({ str => toMultiLine(str, linesCRS, rasterCRS) })

        RasterLinesJobParams(polygon, lines, rasterLayerIds)

      case _ =>
        throw new Exception("Unknown Job Type")
    }
  }

  def rasterLinesJoin(rasterLayers: Seq[TileLayerRDD[SpatialKey]], lines: Seq[MultiLine]): Map[Seq[Int], Int] = {
    val rtree = new STRtree

    lines.foreach({ multiLineString =>
      multiLineString.lines.foreach({ lineString =>
        val Extent(xmin, ymin, xmax, ymax) = lineString.envelope
        rtree.insert(new Envelope(xmin, xmax, ymin, ymax), lineString)
      })
    })

    val joinedRaster = rasterLayers.length match {
      case 1 =>
        rasterLayers.head
          .map({ case (k, v) => (k, List(v)) })
      case 2 =>
        rasterLayers.head.join(rasterLayers.tail.head)
          .map({ case (k, (v1, v2)) => (k, List(v1, v2)) })
      case 3 =>
        rasterLayers.head.join(rasterLayers.tail.head).join(rasterLayers.tail.tail.head)
          .map({ case (k, ((v1, v2), v3)) => (k, List(v1, v2, v3)) })

      case 0 => throw new Exception("At least 1 raster must be specified")
      case _ => throw new Exception("At most 3 rasters can be specified")
    }

    joinedRaster
      .map({ case (key, tiles) =>
        // We calculate extent using the first layer, since the joinedRasters
        // don't have metadata or mapTransform defined on them. The extent will
        // be the same for all layers since they are all in the same projection
        val extent = rasterLayers.head.metadata.mapTransform(key)

        // Similarly, we calculate rasterExtent using the first layer's tiles
        val rasterExtent = RasterExtent(extent, tiles.head.cols, tiles.head.rows)

        val Extent(xmin, ymin, xmax, ymax) = extent
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
          .distinct.map({ case (col, row) => tiles.map({ tile => tile.get(col, row) }) })
          .groupBy(identity).map({ case (k, list) => k -> list.length })
          .toList
      })
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k, list) => k -> list.map(_._2).sum })
  }
}
