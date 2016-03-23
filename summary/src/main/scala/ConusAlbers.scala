package org.wikiwatershed.mmw.geoprocessing

import geotrellis.proj4._


object ConusAlbers extends CRS {
  lazy val proj4jCrs = factory.createFromName("EPSG:5070")

  def epsgCode: Option[Int] = CRS.getEPSGCode(toProj4String + " <>")
}
