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

case class RasterVectorJobParams(
  polygon: Seq[MultiPolygon],
  vector: Seq[Line],
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
        rasterVectorJoin(rasterLayer, vector)
       case _ =>
         throw new Exception("Unknown Job Type")
    }
  }

  def parseConfig(config: Config): MapshedJobParams = {
    val crs: String => geotrellis.proj4.CRS = getCRS(config, _)

    config.getString("input.operationType") match {
      case "RasterVectorJoin" =>
        val rasterCRS = crs("input.rasterCRS")
        val polygonCRS = crs("input.polygonCRS")
        val vectorCRS = crs("input.vectorCRS")
        val polygon = config.getStringList("input.polygon").asScala.map({ str => parseGeometry(str, polygonCRS, rasterCRS) })
        val vector = config.getStringList("input.vector").asScala.map({ str => str.parseJson.convertTo[Line].reproject(vectorCRS, rasterCRS) })
        val zoom = config.getInt("input.zoom")
        val rasterLayerId = LayerId(config.getString("input.raster"), zoom)

        RasterVectorJobParams(polygon, vector, rasterLayerId)
      case _ =>
        throw new Exception("Unknown Job Type")
    }
  }


  def rasterVectorJoin(rasterLayer: TileLayerRDD[SpatialKey], vector: Seq[Line]): Map[Int, Int] = {
    val rtree = new STRtree

    vector.foreach({lineString =>
      val Extent(xmin, ymin, xmax, ymax) = lineString.envelope
      rtree.insert(new Envelope(xmin, xmax, ymin, ymax), lineString)
    })

    rasterLayer
      .map({ case (key, tile) => {
        val metadata = rasterLayer.metadata
        val mapTransform = metadata.mapTransform
        val extent = mapTransform(key)
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

  def rasterVectorJoinSimple(raster: TileLayerRDD[SpatialKey], vector: Seq[Line]): Map[Int, Int] = {
    raster map { case (key, tile) =>
      val extent = raster.metadata mapTransform key
      val rasterExtent = RasterExtent(extent, tile.cols, tile.rows)
      val counts = mutable.Map.empty[Int, Int]
      val pixels = mutable.Set.empty[(Int, Int)]

      vector foreach {lineString =>
        lineString & extent match {
          case LineResult(line) =>
            Rasterizer.foreachCellByLineString(line, rasterExtent)(
              new Callback {
                def apply(col: Int, row: Int): Unit = {
                  pixels += ((col, row))
                }
              }
            )
          case _ =>
        }
      }

      pixels foreach {case (col, row) => {
        val nlcd = tile.get(col, row)
        counts += nlcd -> (counts.getOrElse(nlcd, 0) + 1)
      }}

      counts.toMap
    } reduce { (left, right) =>
      left ++ (right map { case (key, count) =>
        key -> (count + left.getOrElse(key, 0))
      })
    }
  }
}
