package org.wikiwatershed.mmw.geoprocessing

import geotrellis.proj4.{ConusAlbers, LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io._

import com.typesafe.config.Config
import org.apache.spark.SparkContext


/**
  * Collection of common utilities to be used by SparkJobs
  */
trait JobUtils {
  /**
    * Transform the incoming GeoJSON into a [[MultiPolygon]] in the
    * destination CRS.
    *
    * @param   geoJson  The incoming geometry
    * @param   srcCRS   The CRS that the incoming geometry is in
    * @param   destCRS  The CRS that the outgoing geometry should be in
    * @return           A MultiPolygon
    */
  def parseGeometry(geoJson: String, srcCRS: geotrellis.proj4.CRS, destCRS: geotrellis.proj4.CRS): MultiPolygon = {
    import spray.json._

    geoJson.parseJson.convertTo[Geometry] match {
      case p: Polygon => MultiPolygon(p.reproject(srcCRS, destCRS))
      case mp: MultiPolygon => mp.reproject(srcCRS, destCRS)
      case _ => MultiPolygon()
    }
  }

  /**
    * Fetch a particular layer from the catalog, restricted to the
    * given extent, and return a [[TileLayerRDD]] of the result.
    *
    * @param   catalog  The S3 location from which the data should be read
    * @param   layerId  The layer that should be read
    * @param   extent   The extent (subset) of the layer that should be read
    * @return           An RDD of [[SpatialKey]]s
    */
  def queryAndCropLayer(catalog: S3LayerReader, layerId: LayerId, extent: Extent): TileLayerRDD[SpatialKey] = {
    catalog.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
      .where(Intersects(extent))
      .result
  }

  /**
    * Return an [[S3LayerReader]] object to read from the catalog
    * directory in the azavea datahub.
    *
    * @return  An S3LayerReader object
    */
  def catalog(sc: SparkContext): S3LayerReader =
    catalog("azavea-datahub", "catalog")(sc)

  /**
    * Take a bucket and a catalog, and return an [[S3LayerReader]]
    * object to read from it.
    *
    * @param   bucket    The name of the S3 bucket
    * @param   rootPath  The name of the catalog (child of the root directory) in the bucket
    * @return            An S3LayerReader object
    */
  def catalog(bucket: String, rootPath: String)(implicit sc: SparkContext): S3LayerReader = {
    val attributeStore = new S3AttributeStore(bucket, rootPath)
    val catalog = new S3LayerReader(attributeStore)

    catalog
  }

  /**
    * For a given config, return a function that can find the value of a key
    * if it exists in the config.
    *
    * @param   config    The config containing keys
    * @return            A function String => Option[String] that takes a key
    *                    and returns its value if it exists, else None
    */
  def getOptionalFn(config: Config) = (key: String) => {
    config.hasPath(key) match {
      case true => Option(config.getString(key))
      case false => None
    }
  }

  /**
    * For a given config and CRS key, return one of several recognized
    * [[geotrellis.proj4.CRS]]s, or raise an error.
    *
    * @param  config  The config
    * @param  key     The key (e.g. "input.rasterCRS")
    */
  def getCRS(config: Config, key: String) = {
    config.getString(key) match {
      case "LatLng" => LatLng
      case "WebMercator" => WebMercator
      case "ConusAlbers" => ConusAlbers
      case s: String => throw new Exception(s"Unknown CRS: $s")
    }
  }
}
