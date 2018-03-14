package org.wikiwatershed.mmw.geoprocessing

import java.util.concurrent.atomic.{LongAdder, DoubleAdder, DoubleAccumulator}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import collection.concurrent.TrieMap

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._

import geotrellis.spark._

import cats.implicits._

trait Geoprocessing extends Utils {

  @throws(classOf[MissingStreamLinesException])
  @throws(classOf[MissingTargetRasterException])
  def getMultiOperations(input: MultiInput): Future[Map[String, Map[String, Map[String, Double]]]] = {
    val hucs = input.shapes.map(huc => huc.id -> normalizeHuc(huc)).toMap

    // val rasters = collectRastersForInput(input, hucs.map(_._2))
    val futureRasters: Map[String, Future[TileLayerCollection[SpatialKey]]] = {
    val shapes = hucs.map(_._2)
    val aoi = shapes.unionGeometries.asMultiPolygon.get
    val ops = input.operations
    val rasterIds = ops.flatMap(_.targetRaster) ++ ops.flatMap(_.rasters)

      rasterIds.distinct.map { rid =>
        rid -> Future(cropSingleRasterToAOI(rid, 0, aoi))
      }.toMap
    }
    // Note: Op is to be avoided naming because ... well everything is an Op of some kind
    val cachedOpts = input.operations.map(op => op -> getRasterizerOptions(op.pixelIsArea)).toMap

    val result0: Map[String, Map[String, Future[Map[String,Double]]]] = 
      hucs.mapValues { shape =>
        input.operations
          .map(op => op.label -> op).toMap
          .mapValues { op =>
            val opts = cachedOpts(op)
            
            val futureLayers: Future[List[TileLayerCollection[SpatialKey]]] =
              op.rasters.map(futureRasters(_)).sequence
            val futureTargetLayer =
              op.targetRaster.map(futureRasters(_)).sequence

            for {
              layers <- futureLayers
              targetLayer <- futureTargetLayer
            } yield {
              op.name match {
                case "RasterGroupedCount" =>
                  rasterGroupedCount(layers, shape, opts).mapValues(_.toDouble)

                case "RasterGroupedAverage" =>
                  targetLayer match {
                    case Some(tl) =>
                      if (layers.isEmpty) rasterAverage(tl, shape, opts)
                      else rasterGroupedAverage(layers, tl, shape, opts)
                    case None =>
                      throw new MissingTargetRasterException
                  }

                case "RasterLinesJoin" =>
                  input.streamLines match {
                    case Some(mls) => {
                      val lines = (parseMultiLineString(mls) & shape).asMultiLine.get
                      rasterLinesJoin(layers, Seq(lines)).mapValues(_.toDouble)
                    }
                    case None =>
                      throw new MissingStreamLinesException
                  }
              }
            }
          }
      }
    

    val result1: Map[String, Future[Map[String,Map[String,Double]]]] = 
      result0.map { case (k, v) => k -> sequenceMap(v) }

    val result2: Future[Map[String,Map[String,Map[String,Double]]]] = 
      sequenceMap(result1)
    
    result2
  }

  /**
    * For an InputData object, return a histogram of raster grouped count results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  def getRasterGroupedCount(input: InputData): Future[ResultInt] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val opts = getRasterizerOptions(input.pixelIsArea)

    futureLayers.map { layers =>
      ResultInt(rasterGroupedCount(layers, aoi, opts))
    }
  }

  /**
    * For an InputData object, return a histogram of raster grouped average
    * results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  @throws(classOf[MissingTargetRasterException])
  def getRasterGroupedAverage(input: InputData): Future[ResultDouble] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val targetLayer = input.targetRaster match {
      case Some(targetRaster) =>
        cropSingleRasterToAOI(targetRaster, input.zoom, aoi)
      case None => throw new MissingTargetRasterException
    }
    val opts = getRasterizerOptions(input.pixelIsArea)

    futureLayers.map { rasterLayers =>
      val average =
        if (rasterLayers.isEmpty) rasterAverage(targetLayer, aoi, opts)
        else rasterGroupedAverage(rasterLayers, targetLayer, aoi, opts)

      ResultDouble(average)
    }
  }

  /**
    * For an InputData object, return a histogram of raster lines join results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  @throws(classOf[MissingVectorException])
  @throws(classOf[MissingVectorCRSException])
  def getRasterLinesJoin(input: InputData): Future[ResultInt] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val lines = input.vector match {
      case Some(vector) =>
        input.vectorCRS match {
          case Some(crs) =>
            cropLinesToAOI(
              createMultiLineFromInput(vector, crs, input.rasterCRS), aoi)
          case None => throw new MissingVectorCRSException
        }
      case None => throw new MissingVectorException
    }

    futureLayers.map { rasterLayers =>
      ResultInt(rasterLinesJoin(rasterLayers, lines))
    }
  }


  /**
    * For an InputData object, returns a sequence of maps of min, avg, and max
    * values for each raster, in the order of the input rasters
    *
    * @param   input  The InputData
    * @return         Seq of map of min, avg, and max values
    */
  def getRasterSummary(input: InputData): Future[ResultSummary] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val opts = getRasterizerOptions(input.pixelIsArea)

    futureLayers.map { layers =>
      ResultSummary(rasterSummary(layers, aoi, opts))
    }
  }

  private case class TilePixel(key: SpatialKey, col: Int, row: Int)

  /**
    * Given a collection of rasterLayers & a collection of lines, return the
    * values intersected by the rasterized lines.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   lines         A sequence of MultiLines
    * @return                A map of pixel counts
    */
  private def rasterLinesJoin(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    lines: Seq[MultiLine]
  ): Map[String, Int] = {
    val metadata = rasterLayers.head.metadata
    val pixelGroups: TrieMap[(List[Int], TilePixel), Int] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent = metadata.mapTransform(key)
        val re = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        lines.par.foreach({ multiLine =>
          Rasterizer.foreachCellByMultiLineString(multiLine, re) { case (col, row) =>
            val pixelGroup: (List[Int], TilePixel) =
              (tiles.map(_.get(col, row)).toList, TilePixel(key, col, row))
            pixelGroups.getOrElseUpdate(pixelGroup, 1)
          }
        })
      })

    pixelGroups
      .groupBy(_._1._1.toString)
      .mapValues(_.size)
  }

  /**
    * Return the average pixel value from a target raster and a MultiPolygon
    * area of interest.
    *
    * @param   targetLayer   The target TileLayerCollection
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A one element map averaging the pixel values
    */
  private def rasterAverage(
    targetLayer: TileLayerCollection[SpatialKey],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Map[String, Double] = {
    val update = (newValue: Double, pixelValue: (DoubleAdder, LongAdder)) => {
      pixelValue match {
        case (accumulator, counter) => accumulator.add(newValue); counter.increment()
      }
    }

    val metadata = targetLayer.metadata
    val pixelValue = ( new DoubleAdder, new LongAdder )

    targetLayer.par.foreach({ case (key, tile) =>
      val re = RasterExtent(metadata.mapTransform(key), metadata.layout.tileCols,
        metadata.layout.tileRows)

      Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
        val targetLayerData = tile.getDouble(col, row)

        val targetLayerValue =
          if (isData(targetLayerData)) targetLayerData
          else 0.0

        update(targetLayerValue, pixelValue)
      }
    })

    pixelValue match {
      case (accumulator, counter) => Map("List(0)" -> accumulator.sum / counter.sum)
    }
  }

  /**
    * Return the average pixel value from a target raster and a MultiPolygon
    * area of interest.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   targetLayer   The target TileLayerCollection
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A map of targetRaster pixel value averages
    */
  private def rasterGroupedAverage(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    targetLayer: TileLayerCollection[SpatialKey],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Map[String, Double] = {
    val init = () => ( new DoubleAdder, new LongAdder )
    val update = (newValue: Double, pixelValue: (DoubleAdder, LongAdder)) => {
      pixelValue match {
        case (accumulator, counter) => accumulator.add(newValue); counter.increment()
      }
    }

    val metadata = targetLayer.metadata
    val pixelGroups: TrieMap[List[Int], (DoubleAdder, LongAdder)] = TrieMap.empty

    joinCollectionLayers(targetLayer +: rasterLayers).par
      .foreach({ case (key, targetTile :: tiles) =>
        val extent: Extent = metadata.mapTransform(key)
        val re: RasterExtent = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
          val pixelKey: List[Int] = tiles.map(_.get(col, row)).toList
          val pixelValues = pixelGroups.getOrElseUpdate(pixelKey, init())
          val targetLayerData = targetTile.getDouble(col, row)

          val targetLayerValue =
            if (isData(targetLayerData)) targetLayerData
            else 0.0

          update(targetLayerValue, pixelValues)
        }
      })

    pixelGroups
      .mapValues { case (accumulator, counter) => accumulator.sum / counter.sum }
      .map { case (k, v) => k.toString -> v }
      .toMap
  }

  /**
    * From a sequence of rasterLayers and a shape, return a list of pixel counts.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A Map of cell counts
    */
  private def rasterGroupedCount(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Map[String, Int] = {
    val init = () => new LongAdder
    val update = (_: LongAdder).increment()
    // assume all the layouts are the same
    val metadata = rasterLayers.head.metadata

    val pixelGroups: TrieMap[List[Int], LongAdder] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent: Extent = metadata.mapTransform(key)
        val re: RasterExtent = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
          val pixelGroup: List[Int] = tiles.map(_.get(col, row)).toList
          val acc = pixelGroups.getOrElseUpdate(pixelGroup, init())
          update(acc)
        }
      })

    pixelGroups
      .mapValues(_.sum().toInt)
      .map { case (k, v) => k.toString -> v}
      .toMap
  }

  type RasterSummary = (DoubleAccumulator, DoubleAdder, DoubleAccumulator, LongAdder)

  /**
    * From a list of rasters and a shape, return a list of maps containing min,
    * avg, and max values of those rasters.
    *
    * @param   rasterLayers  A sequence of TileLayerCollections
    * @param   multiPolygon  The AOI as a MultiPolygon
    * @return                A Seq of Map of min, avg, and max values
    */
  private def rasterSummary(
    rasterLayers: Seq[TileLayerCollection[SpatialKey]],
    multiPolygon: MultiPolygon,
    opts: Rasterizer.Options
  ): Seq[Map[String, Double]] = {
    val update = (newValue: Double, rasterSummary: RasterSummary) => {
      rasterSummary match {
        case (min, sum, max, count) =>
          min.accumulate(newValue)
          sum.add(newValue)
          max.accumulate(newValue)
          count.increment()
      }
    }

    val init = () => (
      new DoubleAccumulator(new MinWithoutNoData, Double.MaxValue),
      new DoubleAdder,
      new DoubleAccumulator(new MaxWithoutNoData, Double.MinValue),
      new LongAdder
    )

    // assume all layouts are the same
    val metadata = rasterLayers.head.metadata

    val layerSummaries: TrieMap[Int, RasterSummary] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent = metadata.mapTransform(key)
        val re = RasterExtent(extent, metadata.tileLayout.tileCols, metadata.tileLayout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re, opts) { case (col, row) =>
          val pixels: List[Double] = tiles.map(_.getDouble(col, row)).toList
          pixels.zipWithIndex.foreach { case (pixel, index) =>
            val rasterSummary = layerSummaries.getOrElseUpdate(index, init())
            update(pixel, rasterSummary)
          }
        }
      })

    layerSummaries
      .toSeq
      .sortBy(_._1)
      .map { case (_, (min, sum, max, count)) =>
        Map(
          "min" -> min.get(),
          "avg" -> sum.sum() / count.sum(),
          "max" -> max.get()
        )
      }
  }
}
