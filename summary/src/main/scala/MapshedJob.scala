package org.wikiwatershed.mmw.geoprocessing

import geotrellis.raster._
import geotrellis.raster.rasterize.{Callback, Rasterizer}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerRDD}
import geotrellis.vector._

import com.typesafe.config.Config
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * A [[SparkJob]]-derived object for use with Spark Job Server.
  */
object MapshedJob extends SparkJob with JobUtils {

  /**
    * The validation method.  Takes a spark context and a
    * configuration, answers whether or not they constitute a
    * (potentially) valid job.
    *
    * @param  sc      The spark context
    * @param  config  The job configuration
    * @return         An indication of whether or not the job is valid
    */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation =
    SparkJobValid

  /**
    * The 'runJob' method takes a spark context and a configuration
    * and attempts to run the job.
    *
    * @param  sc      The spark context
    * @param  config  The job configuration
    */
  override def runJob(sc: SparkContext, config: Config): Any = {
    config.getString("input.operationType") match {
      case "RasterLinesJoin" =>
        val (rasterLayerIds, lines, polygon) = parseLinesJoinConfig(config)
        val rasterLayers = toLayers(rasterLayerIds, polygon, sc)
        rasterLinesJoin(rasterLayers, lines, sc)

      case "RasterLinesJoinSequential" =>
        val (rasterLayerIds, lines, polygon) = parseLinesJoinConfig(config)
        val rasterLayers = toLayers(rasterLayerIds, polygon, sc)
        rasterLinesJoinSequential(rasterLayers, lines)

      case "RasterGroupedCount" =>
        val (rasterLayerIds, _, polygon) = parseGroupedConfig(config)
        val rasterLayers = toLayers(rasterLayerIds, polygon, sc)
        rasterGroupedCount(rasterLayers, polygon)

      case "RasterGroupedSum" =>
        val (rasterLayerIds, targetLayerId, polygon) = parseGroupedConfig(config)
        val (rasterLayers, targetLayer) = toLayers(rasterLayerIds, targetLayerId.get, polygon, sc)
        rasterGroupedSum(rasterLayers, targetLayer, polygon)

      case "RasterGroupedAverage" =>
        val (rasterLayerIds, targetLayerId, polygon) = parseGroupedConfig(config)
        val (rasterLayers, targetLayer) = toLayers(rasterLayerIds, targetLayerId.get, polygon, sc)
        if (rasterLayers.isEmpty) {
          rasterAverage(targetLayer, polygon)
        } else {
          rasterGroupedAverage(rasterLayers, targetLayer, polygon)
        }

      case _ => throw new Exception("Unknown Job Type")
    }
  }

  /**
    * Perform a join between some rasters and some lines.  Given a
    * collection of rasters and a collection of lines, return the
    * pixel (or multi-pixel) values that are intersected by the
    * rasterized lines.
    *
    * @param  rasterLayers  A sequence of [[TileLayerRDD]] raster layers
    * @param  lines         A sequence of (multi-)lines
    * @param  sc            The spark context (needed for creating RDD)
    */
  def rasterLinesJoin(
    rasterLayers: Seq[TileLayerRDD[SpatialKey]],
    lines: Seq[MultiLine],
    sc: SparkContext
  ): Map[Seq[Int], Int] = {

    val _rtree = new STRtree
    lines.foreach({ multiLineString =>
      val Extent(xmin, ymin, xmax, ymax) = multiLineString.envelope
      _rtree.insert(new Envelope(xmin, xmax, ymin, ymax), multiLineString)
    })

    val rtree = sc.broadcast(_rtree)

    val mt = rasterLayers.head.metadata.mapTransform

    joinRasters(rasterLayers)
      .map({ case (key, tiles) =>
        val extent = mt(key)
        val rasterExtent = RasterExtent(extent, tiles.head.cols, tiles.head.rows)
        val Extent(xmin, ymin, xmax, ymax) = extent
        val envelope = new Envelope(xmin, xmax, ymin,ymax)
        val list = rtree.value.query(envelope).asScala.map(_.asInstanceOf[MultiLine])

        val pixels = mutable.ListBuffer.empty[(Int, Int)]
        val cb = new Callback {
          def apply(col: Int, row: Int): Unit = {
            val pixel = (col, row)
            pixels += pixel
          }
        }

        list.foreach({ multiLine =>
          Rasterizer.foreachCellByMultiLineString(multiLine, rasterExtent)(cb)
        })

        pixels
          .distinct.map({ case (col, row) => tiles.map({ tile => tile.get(col, row) }) })
          .groupBy(identity).map({ case (k, list) => k -> list.length })
          .toList
      })
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k, list) => k -> list.map(_._2).sum })
  }

  /**
    * Perform a join between some rasters and some lines.  Given a
    * collection of rasters and a collection of lines, return the
    * pixel (or multi-pixel) values that are intersected by the
    * rasterized lines.
    *
    * This differs from 'rasterLinesJoin' in that it operates in an
    * intentionally sequential fashion.
    *
    * @param  rasterLayers  A sequence of [[TileLayerRDD]] raster layers
    * @param  lines         A sequence of (multi-)lines
    */
  def rasterLinesJoinSequential(
    rasterLayers: Seq[TileLayerRDD[SpatialKey]],
    lines: Seq[MultiLine]
  ): Map[Seq[Int], Int] = {

    val rtree = new STRtree
    lines.foreach({ multiLineString =>
      val Extent(xmin, ymin, xmax, ymax) = multiLineString.envelope
      rtree.insert(new Envelope(xmin, xmax, ymin, ymax), multiLineString)
    })

    val mt = rasterLayers.head.metadata.mapTransform

    joinRasters(rasterLayers)
      .collect
      .map({ case (key, tiles) =>
        val extent = mt(key)
        val rasterExtent = RasterExtent(extent, tiles.head.cols, tiles.head.rows)
        val Extent(xmin, ymin, xmax, ymax) = extent
        val envelope = new Envelope(xmin, xmax, ymin,ymax)
        val list = rtree.query(envelope).asScala.map(_.asInstanceOf[MultiLine])

        val pixels = mutable.ListBuffer.empty[(Int, Int)]
        val cb = new Callback {
          def apply(col: Int, row: Int): Unit = {
            val pixel = (col, row)
            pixels += pixel
          }
        }

        list.foreach({ multiLine =>
          Rasterizer.foreachCellByMultiLineString(multiLine, rasterExtent)(cb)
        })

        pixels
          .distinct.map({ case (col, row) => tiles.map({ tile => tile.get(col, row) }) })
          .groupBy(identity).map({ case (k, list) => k -> list.length })
          .toList
      })
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k, list) => k -> list.map(_._2).sum })
  }

  /**
    * Perform a join between some rasters and some polygons.  Given a
    * collection of rasters and a collection of polygons, return the
    * pixel (or multi-pixel) values that are covered by the rasterized
    * shapes.
    *
    * @param  rasterLayers   A sequence of [[TileLayerRDD]] raster layers
    * @param  multiPolygons  A sequence of (multi-)polygons
    */
  def rasterGroupedCount(
    rasterLayers: Seq[TileLayerRDD[SpatialKey]],
    multiPolygons: Seq[MultiPolygon]
  ): Map[Seq[Int], Int] = {
    joinRasters(rasterLayers)
      .map({ case (key, tiles) =>
        getDistinctPixels(rasterLayers.head, key, tiles.head, multiPolygons)
          .map({ case (col, row) => tiles.map({ tile => tile.get(col, row) }) })
          .groupBy(identity).map({ case (k, list) => k -> list.length })
          .toList
      })
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k, list) => k -> list.map(_._2).sum })
  }

  /**
    * Perform a join between some rasters and some polygons.  Given a
    * collection of rasters, a collection of polygons, and a target raster,
    * return the sum of pixel (or multi-pixel) values of the target raster
    * that are covered by the rasterized shapes, grouped by the keys of
    * raster layers.
    *
    * @param  rasterLayers   A sequence of [[TileLayerRDD]] raster layers
    * @param  targetLayer    A [[TileLayerRDD]] target layer to aggregate
    * @param  multiPolygons  A sequence of (multi-)polygons
    */
  def rasterGroupedSum(
    rasterLayers: Seq[TileLayerRDD[SpatialKey]],
    targetLayer: TileLayerRDD[SpatialKey],
    multiPolygons: Seq[MultiPolygon]
  ): Map[Seq[Int], Double] = {
    targetLayer.join(joinRasters(rasterLayers))
      .map({ case (key, (targetTile, rasterTiles)) =>
        getDistinctPixels(targetLayer, key, targetTile, multiPolygons)
          .map({ case (col, row) =>
            val floatVal = targetTile.getDouble(col, row)
            (
              rasterTiles.map({ tile => tile.get(col, row) }),
              if (isData(floatVal)) floatVal else 0.0
            )
          })
          .groupBy(_._1).map({ case (k, list) => k -> list.map(_._2).sum })
          .toList
      })
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k, list) => k -> list.map(_._2).sum })
  }

  /**
    * Perform a join between some rasters and some polygons.  Given a
    * collection of rasters, a collection of polygons, and a target raster,
    * return the average of pixel (or multi-pixel) values of the target raster
    * that are covered by the rasterized shapes, grouped by the keys of
    * raster layers.
    *
    * @param  rasterLayers   A sequence of [[TileLayerRDD]] raster layers
    * @param  targetLayer    A [[TileLayerRDD]] target layer to aggregate
    * @param  multiPolygons  A sequence of (multi-)polygons
    */
  def rasterGroupedAverage(
    rasterLayers: Seq[TileLayerRDD[SpatialKey]],
    targetLayer: TileLayerRDD[SpatialKey],
    multiPolygons: Seq[MultiPolygon]
  ): Map[Seq[Int], Double] = {
    targetLayer.join(joinRasters(rasterLayers))
      .map({ case (key, (targetTile, rasterTiles)) =>
        getDistinctPixels(targetLayer, key, targetTile, multiPolygons)
          .map({ case (col, row) =>
            val floatVal = targetTile.getDouble(col, row)
            (
              rasterTiles.map({ tile => tile.get(col, row) }),
              if (isData(floatVal)) floatVal else 0.0
            )
          })
          .groupBy(_._1).map({ case (k, list) =>
            k -> (list.map(_._2).sum, list.map(_._2).length)
          })
          .toList
      })
      .reduce({ (left, right) => left ++ right})
      .groupBy(_._1).map({ case (k, list) =>
        k -> list.map(_._2._1).sum / list.map(_._2._2).sum
      })
  }

  /**
    * Given a target raster layer and a sequence of multiPolygons, averages the
    * value of the raster layer clipped to those multiPolygons. Returns the
    * results as the first value of the first key, to maintain API conventions.
    *
    * @param  targetLayer    A [[TileLayerRDD]] raster layer to average over
    * @param  multiPolygons  A sequence of (multi-)polygons
    */
  def rasterAverage(
    targetLayer: TileLayerRDD[SpatialKey],
    multiPolygons: Seq[MultiPolygon]
  ): Map[Seq[Int], Double] = {
    val (totalSum, totalCount) = targetLayer.map({ case (key, tile) =>
      val distinctPixels = getDistinctPixels(targetLayer, key, tile, multiPolygons)

      val count = distinctPixels.length
      val sum = distinctPixels.map({ case (col, row) =>
        val floatVal = tile.getDouble(col, row)
        if (isData(floatVal)) floatVal else 0.0
      }).sum

      (sum, count)
    })
    .reduce({ case ((sum1, count1), (sum2, count2)) =>
      (sum1 + sum2, count1 + count2)
    })

    Map(List(0) -> totalSum / totalCount)
  }
}
