package org.wikiwatershed.mmw

import geotrellis.raster._
import geotrellis.spark._

package object geoprocessing {
  type TileLayerSeq[K] = Seq[(K,Tile)] with Metadata[TileLayerMetadata[K]]
}
