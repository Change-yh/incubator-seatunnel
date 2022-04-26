package org.apache.seatunnel.spark.source

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfigThrowable
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.Config._
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Trifile extends SparkBatchSource {

  override def checkConfig(): CheckResult = {
    // checkAllExists(config, PATH, FORMAT)
    checkAllExists(config, PATH)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        FORMAT -> DEFAULT_FORMAT,
        HEADER -> DEFAULT_HEADER,
        INFER_SCHEMA -> DEFAULT_INFER_SCHEMA,
        DELIMITER -> DEFAULT_DELIMITER
        // NULL_VALUE -> DEFAULT_NULL_VALUER
      ))
    config = config.withFallback(defaultConfig)
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {

    val path = config.getString(PATH)
    val format = config.getString(FORMAT)
    val reader = env.getSparkSession.read.format(format)

    val header = config.getString(HEADER)
    val inferSchema = config.getString(INFER_SCHEMA)
    val delimiter = config.getString(DELIMITER)
    // val nullValue = config.getString(NULL_VALUE)

    val options = Map(
      "header" -> header,
      "inferSchema" -> inferSchema,
      "delimiter" -> delimiter,
      "encoding" -> "gbk"
      // 该配置好像无效
      // "nullValue"->nullValue
    )

    Try(extractSubConfigThrowable(config, OPTION_PREFIX, false)) match {
      case Success(options) => options.entrySet().foreach(e => {
        reader.option(e.getKey, String.valueOf(e.getValue.unwrapped()))
      })
      case Failure(_) =>
    }

    format match {
      case TEXT => reader.text(path) // .as("value")
      case PARQUET =>
        val frame = reader.option("encoding", "utf-8").parquet(path)
        frame.show()
        frame

      case JSON => reader.json(path)
      case ORC => reader.orc(path)
      case CSV => reader.options(options).csv(path)
      case _ => reader.format(format).load(path)
    }
  }
}
