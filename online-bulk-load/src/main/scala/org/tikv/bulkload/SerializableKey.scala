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

package org.tikv.bulkload

import java.util

import org.tikv.common.codec.KeyUtils
import org.tikv.common.key.Key
import org.tikv.common.util.FastByteComparisons

case class SerializableKey(val bytes: Array[Byte])
  extends Comparable[SerializableKey]
    with Serializable {
  override def toString: String = LogDesensitization.hide(KeyUtils.formatBytes(bytes))

  override def equals(that: Any): Boolean =
    that match {
      case that: SerializableKey => this.bytes.sameElements(that.bytes)
      case _ => false
    }

  override def hashCode(): Int =
    util.Arrays.hashCode(bytes)

  override def compareTo(o: SerializableKey): Int = {
    FastByteComparisons.compareTo(bytes, o.bytes)
  }

  def getRowKey: Key = {
    Key.toRawKey(bytes)
  }
}

object LogDesensitization {
  private val enableLogDesensitization = getLogDesensitization

  def hide(info: String): String = if (enableLogDesensitization) "*"
  else info

  /**
   * TiSparkLogDesensitizationLevel = 1 => disable LogDesensitization, otherwise enable
   * LogDesensitization
   *
   * @return true enable LogDesensitization, false disable LogDesensitization
   */
  private def getLogDesensitization: Boolean = {
    val tiSparkLogDesensitizationLevel = "TiSparkLogDesensitizationLevel"
    var tmp = System.getenv(tiSparkLogDesensitizationLevel)
    if (tmp != null && !("" == tmp)) return !("1" == tmp)
    tmp = System.getProperty(tiSparkLogDesensitizationLevel)
    if (tmp != null && !("" == tmp)) return !("1" == tmp)
    true
  }
}