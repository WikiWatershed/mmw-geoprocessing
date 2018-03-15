package org.wikiwatershed.mmw

import geotrellis.spark._

package object geoprocessing {
  type HucID = String
  type OperationID = String
  type RasterID = String
  type RasterLayer = TileLayerCollection[SpatialKey]
  type GeoJSONString = String
}
