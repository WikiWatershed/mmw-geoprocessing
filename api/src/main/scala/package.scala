package org.wikiwatershed.mmw

import geotrellis.layer._

package object geoprocessing {
  type HucID = String
  type OperationID = String
  type RasterID = String
  type RasterLayer = TileLayerCollection[SpatialKey]
  type GeoJSONString = String
}
