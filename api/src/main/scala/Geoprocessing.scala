package org.wikiwatershed.mmw.geoprocessing

import java.util.concurrent.atomic.{LongAdder, DoubleAdder}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import collection.concurrent.TrieMap

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._

import geotrellis.spark._

trait Geoprocessing extends Utils {
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
  def getRasterGroupedAverage(input: InputData): Future[ResultDouble] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val targetLayer = input.targetRaster match {
      case Some(targetRaster) =>
        cropSingleRasterToAOI(targetRaster, input.zoom, aoi)
      case None =>
        throw new Exception("Request data missing required 'targetRaster'.")
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
  def getRasterLinesJoin(input: InputData): Future[ResultInt] = {
    val aoi = createAOIFromInput(input)
    val futureLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val lines = input.vector match {
      case Some(vector) =>
        input.vectorCRS match {
          case Some(crs) =>
            createMultiLineFromInput(vector, crs, input.rasterCRS)
          case None =>
            throw new Exception("Request data missing required 'vectorCRS'.")
        }
      case None =>
        throw new Exception("Request data missing required 'vector'.")
    }

    futureLayers.map { rasterLayers =>
      ResultInt(rasterLinesJoin(rasterLayers, lines))
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
}
