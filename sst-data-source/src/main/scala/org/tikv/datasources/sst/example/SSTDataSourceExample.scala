/*
 * Copyright 2021 TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.datasources.sst.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.tikv.shade.com.google.protobuf.ByteString

object SSTDataSourceExample {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    var sstFilePath: String = ""
    var exit: Boolean = true

    if (args.length > 0) {
      sstFilePath = args(0)
    } else {
      throw new Exception("please input path to sst files")
    }

    if (args.length > 1) {
      exit = args(1).toBoolean
    }

    logger.info(s"""
         |*****************
         |sstFilePath=$sstFilePath
         |exit=$exit
         |*****************
         |""".stripMargin)

    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val df = spark.read
      .format("sst")
      .load(sstFilePath)
    logger.info(s"count = ${df.count()}")

    df.printSchema()

    df.show(false)
    df.take(20).foreach { row =>
      val key = row.get(0).asInstanceOf[Array[Byte]]
      val value = row.get(1).asInstanceOf[Array[Byte]]
      logger.info(
        s"${ByteString.copyFrom(key).toStringUtf8}\t${ByteString.copyFrom(value).toStringUtf8}")
    }

    val end = System.currentTimeMillis()
    logger.info(s"total time: ${(end - start) / 1000}s")

    while (!exit) {
      Thread.sleep(1000)
    }
  }
}
