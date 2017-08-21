package org.wikiwatershed.mmw.geoprocessing

import java.util.concurrent.atomic.{LongAdder, DoubleAdder}

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
  def getRasterGroupedCount(input: InputData): ResultInt = {
    val aoi = createAOIFromInput(input)
    val rasterLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    ResultInt(rasterGroupedCount(rasterLayers, aoi))
  }

  /**
    * For an InputData object, return a histogram of raster grouped average
    * results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  def getRasterGroupedAverage(input: InputData): ResultDouble = {
    val aoi = createAOIFromInput(input)
    val rasterLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    val targetLayer = input.targetRaster match {
      case Some(targetRaster) =>
        cropSingleRasterToAOI(targetRaster, input.zoom, aoi)
      case None =>
        throw new Exception("Request data missing required 'targetRaster'.")
    }

    val average =
      if (rasterLayers.isEmpty) rasterAverage(targetLayer, aoi)
      else rasterGroupedAverage(rasterLayers, targetLayer, aoi)

    ResultDouble(average)
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
    multiPolygon: MultiPolygon
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

      Rasterizer.foreachCellByMultiPolygon(multiPolygon, re) { case (col, row) =>
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
    multiPolygon: MultiPolygon
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

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re) { case (col, row) =>
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
    multiPolygon: MultiPolygon
  ): Map[String, Int] = {
    val init = () => new LongAdder
    val update = (_: LongAdder).increment()
    // assume all the layouts are the same
    val metadata = rasterLayers.head.metadata

    var pixelGroups: TrieMap[List[Int], LongAdder] = TrieMap.empty

    joinCollectionLayers(rasterLayers).par
      .foreach({ case (key, tiles) =>
        val extent: Extent = metadata.mapTransform(key)
        val re: RasterExtent = RasterExtent(extent, metadata.layout.tileCols,
            metadata.layout.tileRows)

        Rasterizer.foreachCellByMultiPolygon(multiPolygon, re) { case (col, row) =>
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
