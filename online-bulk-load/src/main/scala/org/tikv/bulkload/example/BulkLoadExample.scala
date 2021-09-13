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

package org.tikv.bulkload.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.tikv.bulkload.RawKVBulkLoader

import scala.util.Random

object BulkLoadExample {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    var pdaddr: String = "127.0.0.1:2379"
    var prefix: String = "test_"
    var size: Long = 1000
    var partition: Int = 10
    var exit: Boolean = true

    val value = "A" * 64

    if (args.length > 0) {
      pdaddr = args(0)
    }

    if (args.length > 1) {
      prefix = args(1)
    }

    if (args.length > 2) {
      size = args(2).toLong
    }

    if (args.length > 3) {
      partition = args(3).toInt
    }

    if(args.length > 4) {
      exit = args(4).toBoolean
    }

    logger.info(
      s"""
         |*****************
         |pdaddr=$pdaddr
         |prefix=$prefix
         |size=$size
         |partition=$partition
         |exit=$exit
         |*****************
         |""".stripMargin)

    if(size / partition > Int.MaxValue) {
      throw new Exception("size / partition > Int.MaxValue")
    }

    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val rdd = spark.sparkContext.makeRDD(0L until partition, partition).flatMap { partitionIndex =>
      val partitionSize = (size / partition).toInt
      val data = new Array[(Array[Byte], Array[Byte])](partitionSize)
      (0 until partitionSize).foreach { i =>
        val key = s"$prefix${genKey(i.toLong + partitionIndex * partitionSize)}"
        data(i) = (key.toArray.map(_.toByte), value.toArray.map(_.toByte))
      }
      Random.shuffle(data.toSeq)
    }.coalesce(partition, true)

    new RawKVBulkLoader(pdaddr).bulkLoad(rdd)

    val end = System.currentTimeMillis()

    logger.info(s"total time: ${(end - start) / 1000}s")

    while(!exit) {
      Thread.sleep(1000)
    }
  }

  private def genKey(i: Long): String = f"$i%012d"
}
