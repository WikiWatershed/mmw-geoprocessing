package org.wikiwatershed.mmw.geoprocessing

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io._

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver._

object ConusAlbers extends CRS {
  lazy val proj4jCrs = factory.createFromName("EPSG:5070")

  def epsgCode: Option[Int] = CRS.getEPSGCode(toProj4String + " <>")
}

case class MapshedJobParams(nlcdLayerId: LayerId, geometry: Seq[MultiPolygon])

/**
  * The "main" object for this module.
  */
object MapshedJob extends SparkJob {
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val params = parseConfig(config)
    val extent = GeometryCollection(params.geometry).envelope
    val nlcdLayer = queryAndCropLayer(catalog(sc), params.nlcdLayerId, extent)

    histogram(nlcdLayer, params.geometry)
  }

  def parseConfig(config: Config): MapshedJobParams = {
    import scala.collection.JavaConverters._

    def getOptional(key: String): Option[String] = {
      config.hasPath(key) match {
        case true => Option(config.getString(key))
        case false => None
      }
    }

    val zoom = config.getInt("input.zoom")
    val nlcdLayer = LayerId(config.getString("input.nlcdLayer"), zoom)
    val tileCRS = getOptional("input.tileCRS") match {
      case Some("LatLng") => LatLng
      case Some("WebMercator") => WebMercator
      case Some("ConusAlbers") => ConusAlbers
      case _ => ConusAlbers
    }
    val polyCRS = getOptional("input.polyCRS") match {
      case Some("LatLng") => LatLng
      case Some("WebMercator") => WebMercator
      case Some("ConusAlbers") => ConusAlbers
      case _ => LatLng
    }
    val geometry = config.getStringList("input.geometry").asScala.map {
      str => parseGeometry(str, polyCRS, tileCRS)
    }

    MapshedJobParams(nlcdLayer, geometry)
  }

  def parseGeometry(geoJson: String, srcCRS: CRS, destCRS: CRS): MultiPolygon = {
    import spray.json._

    geoJson.parseJson.convertTo[Geometry] match {
      case p: Polygon => MultiPolygon(p.reproject(srcCRS, destCRS))
      case mp: MultiPolygon => mp.reproject(srcCRS, destCRS)
      case _ => MultiPolygon()
    }
  }

  def queryAndCropLayer(catalog: S3LayerReader, layerId: LayerId, extent: Extent): TileLayerRDD[SpatialKey] = {
    catalog.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
      .where(Intersects(extent))
      .result
  }

  def catalog(sc: SparkContext): S3LayerReader = {
    catalog("azavea-datahub", "catalog")(sc)
  }

  def catalog(bucket: String, rootPath: String)(implicit sc: SparkContext): S3LayerReader = {
    val attributeStore = new S3AttributeStore(bucket, rootPath)
    val catalog = new S3LayerReader(attributeStore)

    catalog
  }

  def histogram(layer: TileLayerRDD[SpatialKey], multiPolygons: Seq[MultiPolygon]) = {
    import scala.collection.mutable
    import geotrellis.raster.rasterize._

    val histogramParts = layer.map { case (key, tile) =>
      multiPolygons.map { multiPolygon =>
        val extent = layer.metadata.mapTransform(key) // transform spatial key to extent
        val rasterExtent = RasterExtent(extent, tile.cols, tile.rows) // transform extent to raster extent
        val clipped = multiPolygon & extent
        val localHistogram = mutable.Map.empty[Int, Int]

        def intersectionComponentsToHistogram(ps: Seq[Polygon]) = {
          ps.foreach { p =>
            Rasterizer.foreachCellByPolygon(p, rasterExtent)(
              new Callback {
                def apply(col: Int, row: Int): Unit = {
                  val nlcdType = tile.get(col, row)

                  if (!localHistogram.contains(nlcdType)) { localHistogram(nlcdType) = 0 }

                  localHistogram(nlcdType) += 1
                }
              }
            )
          }
        }

        clipped match {
          case PolygonResult(p) => intersectionComponentsToHistogram(List(p))
          case MultiPolygonResult(mp) => intersectionComponentsToHistogram(mp.polygons)
          case _ =>
        }

        localHistogram.toMap
      }
    }

    histogramParts.reduce { (s1, s2) =>
      (s1 zip s2).map { case (left, right) =>
        (left.toSeq ++ right.toSeq)
            .groupBy(_._1)
            .map { case (nlcdType, counts) => (nlcdType, counts.map(_._2).sum)}
      }
    }
  }
}
