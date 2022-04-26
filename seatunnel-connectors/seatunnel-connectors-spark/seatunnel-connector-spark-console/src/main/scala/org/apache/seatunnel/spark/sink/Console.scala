/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.spark.sink

import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.Config.{LIMIT, SERIALIZER, PLAIN, JSON, SCHEMA, DEFAULT_SERIALIZER, DEFAULT_LIMIT}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Console extends SparkBatchSink {

  override def output(df: Dataset[Row], env: SparkEnvironment): Unit = {
//    val limit = config.getInt(LIMIT)
    val limit = config.getInt("limit")

//    config.getString(SERIALIZER) match {
    config.getString("serializer") match {
//      case PLAIN =>
      case "plain" =>
        if (limit == -1) {
          df.show(Int.MaxValue, truncate = false)
        } else if (limit > 0) {
          df.show(limit, truncate = false)
        }
//      case JSON =>
      case "json" =>
        if (limit == -1) {
          // scalastyle:off
          df.toJSON.take(Int.MaxValue).foreach(s => println(s))
          // scalastyle:on
        } else if (limit > 0) {
          // scalastyle:off
          df.toJSON.take(limit).foreach(s => println(s))
          // scalastyle:on
        }
//      case SCHEMA =>
      case "schema" =>
        df.printSchema()
    }
  }

  override def checkConfig(): CheckResult = {
//    if (!config.hasPath(LIMIT) || (config.hasPath(LIMIT) && config.getInt(LIMIT) >= -1)) {
    if (!config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1)) {
      CheckResult.success()
    } else {
//      CheckResult.error("Please specify [" + LIMIT + "] as Number[-1, " + Int.MaxValue + "]")
      CheckResult.error("Please specify [" + "limit" + "] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
//        LIMIT -> DEFAULT_LIMIT,
        "limit" -> 100,
//        SERIALIZER -> DEFAULT_SERIALIZER // plain | json
        "serializer" -> "plain"
      ))
    config = config.withFallback(defaultConfig)
  }
}
