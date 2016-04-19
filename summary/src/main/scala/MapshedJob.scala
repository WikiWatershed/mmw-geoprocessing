package org.wikiwatershed.mmw.geoprocessing

import geotrellis.spark.{LayerId, SpatialKey, TileLayerRDD}
import geotrellis.vector.{Geometry, GeometryCollection, Line, LineResult, MultiPolygon, MultiPolygonResult, Polygon, PolygonResult}
import geotrellis.vector.io._
import com.typesafe.config.Config
import geotrellis.raster._
import geotrellis.raster.rasterize.{Callback, Rasterizer}
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}
import spray.json._


trait MapshedJobParams

case class RasterVectorJobParams(
  polygon: Seq[MultiPolygon],
  vector: Seq[Line],
  rasterLayerId: LayerId
) extends MapshedJobParams

object MapshedJob extends SparkJob with JobUtils {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    // TODO Add real validation
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
     parseConfig(config) match {
      case RasterVectorJobParams(polygon, vector, rasterLayerId) =>
        val extent = GeometryCollection(polygon).envelope
        val rasterLayer = queryAndCropLayer(catalog(sc), rasterLayerId, extent)
        rasterVectorJoin(rasterLayer, vector)
      case _ => throw new Exception("Unknown Job Type")
    }
  }

  def parseConfig(config: Config): MapshedJobParams = {
    import scala.collection.JavaConverters._
    import geotrellis.proj4.{LatLng, WebMercator}

    val getOptional = getOptionalFn(config)

    config.getString("input.operationType") match {
      case "RasterVectorJoin" =>
        val rasterCRS = getOptional("input.rasterCRS") match {
          case Some("LatLng") => LatLng
          case Some("WebMercator") => WebMercator
          case Some("ConsuAlbers") => ConusAlbers
          case _ => ConusAlbers
        }
        val polygonCRS = getOptional("input.polygonCRS") match {
          case Some("LatLng") => LatLng
          case Some("WebMercator") => WebMercator
          case Some("ConusAlbers") => ConusAlbers
          case _ => LatLng
        }
        val vectorCRS = getOptional("input.vectorCRS") match {
          case Some("LatLng") => LatLng
          case Some("WebMercator") => WebMercator
          case Some("ConusAlbers") => ConusAlbers
          case _ => LatLng
        }
        val polygon = config.getStringList("input.polygon").asScala.map {
          str => parseGeometry(str, polygonCRS, rasterCRS)
        }
        val vector = config.getStringList("input.vector").asScala.map {
          str => str.parseJson.convertTo[Line].reproject(vectorCRS, rasterCRS)
        }
        val zoom = config.getInt("input.zoom")
        val rasterLayerId = LayerId(config.getString("input.raster"), zoom)

        RasterVectorJobParams(polygon, vector, rasterLayerId)

      case _ =>
        throw new Exception("Unknown Job Type")
    }
  }


  def rasterVectorJoin(rasterLayer: TileLayerRDD[SpatialKey], vector: Seq[Line]): Map[Int, Int] = {

    rasterLayer
      .map({ case (key, tile) => {
        val metadata = rasterLayer.metadata
        val mapTransform = metadata.mapTransform
        val extent = mapTransform(key)
        val rasterExtent = RasterExtent(extent, tile.cols, tile.rows)

        val pixels = mutable.Set.empty[(Int, Int)]
        vector.foreach(lineString =>
          (lineString & extent) match {
            case LineResult(line) =>
              Rasterizer.foreachCellByLineString(line, rasterExtent)(
                new Callback {
                  def apply(col: Int, row: Int): Unit = {
                    val pixel = (col, row)
                    pixels += pixel
                  }
                }
              )
            case _ =>
          }
        )

        pixels.toList.map({ case (col, row) => tile.get(col, row)}).toList
      }})
      .map({ x => println(s"YYY $x"); x })
      .reduce({ (left, right) => left ++ right})
      .groupBy({ z => println(s"ZZZ $z"); z })
      .map({ case (k, v) => (k, v.length)})
  }

  def histograms(nlcd: TileLayerRDD[SpatialKey], soil: TileLayerRDD[SpatialKey], multiPolygons: Seq[MultiPolygon]): Seq[Map[(Int, Int), Int]] = {
    import scala.collection.mutable
    import geotrellis.raster.rasterize.{Rasterizer, Callback}

    val joinedRasters = nlcd.join(soil)
    val histogramParts = joinedRasters.map { case (key, (nlcdTile, soilTile)) =>
      multiPolygons.map { multiPolygon =>
        val extent = nlcd.metadata.mapTransform(key) // transform spatial key to extent
      val rasterExtent = RasterExtent(extent, nlcdTile.cols, nlcdTile.rows) // transform extent to raster extent
      val clipped = multiPolygon & extent
        val localHistogram = mutable.Map.empty[(Int, Int), Int]

        def intersectionComponentsToHistogram(ps : Seq[Polygon]) = {
          ps.foreach { p =>
            Rasterizer.foreachCellByPolygon(p, rasterExtent)(
              new Callback {
                def apply(col: Int, row: Int): Unit = {
                  val nlcdType = nlcdTile.get(col,row)
                  val soilType = soilTile.get(col,row) match {
                    case NODATA => 3
                    case n : Int => n
                  }
                  val pair = (nlcdType, soilType)
                  if(!localHistogram.contains(pair)) { localHistogram(pair) = 0 }
                  localHistogram(pair) += 1
                }
              }
            )
          }
        }

        clipped match {
          case PolygonResult(polygon) => intersectionComponentsToHistogram(List(polygon))
          case MultiPolygonResult(multiPolygon) => intersectionComponentsToHistogram(multiPolygon.polygons)
          case _ =>
        }

        localHistogram.toMap
      }
    }

    histogramParts.reduce { (s1, s2) =>
      (s1 zip s2).map { case (left, right) =>
        (left.toSeq ++ right.toSeq)
          .groupBy(_._1)
          .map { case (nlcdSoilPair, counts) => (nlcdSoilPair, counts.map(_._2).sum) }
      }
    }
  }
}
