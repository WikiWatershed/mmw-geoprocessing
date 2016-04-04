package org.wikiwatershed.mmw.geoprocessing

import com.typesafe.config.Config
import org.apache.spark._
import spark.jobserver._

/**
  * The "main" object for this module.
  */
object MapshedJob extends SparkJob {
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

  }
}
