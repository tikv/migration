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
import org.tikv.bulkload.RawKVBulkLoader

object BulkLoadExample {

  var pdaddr: String = "127.0.0.1:2379"
  var prefix: String = "test_"
  var size: Long = 1000
  var partition: Int = 10

  def main(args: Array[String]): Unit = {
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

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val value = genValue(64)

    val rdd = spark.sparkContext.parallelize(1L to size, partition).map { i =>
      val key = s"${prefix}${genKey(i)}"
      (key.toArray.map(_.toByte), value.toArray.map(_.toByte))
    }
    new RawKVBulkLoader(pdaddr).bulkLoad(rdd)
  }

  private def genKey(i: Long): String = f"$i%012d"

  private def genValue(valueLength: Int): String = "A" * valueLength

}
