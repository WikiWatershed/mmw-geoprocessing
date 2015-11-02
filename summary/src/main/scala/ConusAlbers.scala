package org.wikiwatershed.mmw.geoprocessing

import geotrellis.proj4.CRS;

object ConusAlbers extends CRS {
  lazy val crs = factory.createFromName("EPSG:5070")

  def epsgCode: Option[Int] = CRS.getEPSGCode(toProj4String + " <>")
}
