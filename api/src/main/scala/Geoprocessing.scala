package org.wikiwatershed.mmw.geoprocessing

import java.util.concurrent.atomic.LongAdder

import collection.concurrent.TrieMap

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._
import geotrellis.vector.io._

import geotrellis.spark._

trait Geoprocessing extends Utils {
  /**
    * For an InputData object, return a histogram of raster grouped count results.
    *
    * @param   input  The InputData
    * @return         A histogram of results
    */
  def getRasterGroupedCount(input: InputData): Result = {
    val aoi = createAOIFromInput(input)
    val rasterLayers = cropRastersToAOI(input.rasters, input.zoom, aoi)
    Result(rasterGroupedCount(rasterLayers, aoi))
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
