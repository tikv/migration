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
  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      pdaddr = args(0)
    }
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val value = genValue(64)

    val rdd = spark.sparkContext.parallelize(1 to 1000).map { i =>
      val key = s"test_${genKey(i)}"
      println(key)
      (key.toArray.map(_.toByte), value.toArray.map(_.toByte))
    }
    new RawKVBulkLoader(pdaddr).bulkLoad(rdd)
  }

  private def genKey(i: Int): String = {
    var s = ""

    if(i < 10) {
      s = s + "0000000"
    } else if(i < 100) {
      s = s + "000000"
    } else if(i < 1000) {
      s = s + "00000"
    } else if(i < 10000) {
      s = s + "0000"
    } else if(i < 100000) {
      s = s + "000"
    } else if(i < 1000000) {
      s = s + "00"
    }else if(i < 10000000) {
      s = s + "0"
    }
    s + i
  }

  private def genValue(valueLength: Int): String = {
    var s = ""
    (1 to valueLength).foreach { i =>
      s = s + "A"
    }
    s
  }

}
