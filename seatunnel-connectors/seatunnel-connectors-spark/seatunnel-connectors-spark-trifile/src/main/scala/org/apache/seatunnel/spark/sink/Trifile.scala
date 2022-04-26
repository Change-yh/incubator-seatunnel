package org.apache.seatunnel.spark.sink

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfigThrowable
import org.apache.seatunnel.common.utils.StringTemplate
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.Config._
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import java.util
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class Trifile extends SparkBatchSink {

  override def checkConfig(): CheckResult = {
    checkAllExists(config, PATH)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        PARTITION_BY -> util.Arrays.asList(),
        SAVE_MODE -> SAVE_MODE_ERROR, // allowed values: overwrite, append, ignore, error
        SERIALIZER -> PARQUET, // allowed values: csv, json, parquet, text
        PATH_TIME_FORMAT -> DEFAULT_TIME_FORMAT // if variable 'now' is used in path, this option specifies its time_format
      ))
    config = config.withFallback(defaultConfig)
  }

  override def output(ds: Dataset[Row], env: SparkEnvironment): Unit = {
    val writer = ds.write.mode(config.getString(SAVE_MODE))
    if (config.getStringList(PARTITION_BY).nonEmpty) {
      writer.partitionBy(config.getStringList(PARTITION_BY): _*)
    }

    Try(extractSubConfigThrowable(config, OPTION_PREFIX, false)) match {
      case Success(options) => options.entrySet().foreach(e => {
          writer.option(e.getKey, String.valueOf(e.getValue.unwrapped()))
        })
      case Failure(_) =>
    }

    val path = StringTemplate.substitute(config.getString(PATH), config.getString(PATH_TIME_FORMAT))
    config.getString(SERIALIZER) match {
      case CSV => writer.csv(path)
      case JSON => writer.json(path)
      case PARQUET => writer.parquet(path)
      case TEXT => writer.text(path)
      case ORC => writer.orc(path)
    }
  }
}
