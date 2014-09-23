//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.cba.omnia.edge
package source.memory

import cascading.tuple.Tuple
import com.cba.omnia.edge.test._
import com.twitter.maple.tap.TupleMemoryInputFormat, TupleMemoryInputFormat.TUPLES_PROPERTY
import org.apache.hadoop.mapred._
import scala.collection.JavaConverters._

import au.com.cba.omnia.permafrost.test.Spec

class DistributableMemoryInputFormatSpec extends Spec { def is = s2"""
Distributable Memory Input Format
=================================

Split size should
  default to 1                                    $default
  calculate based on tuple size and mappers       $calculated

"""

  def calculated = prop((size: SmallNatural, mappers: SmallNatural) =>
    splitsFor(size.value, Some(mappers.value)).length must_== math.max(math.min(mappers.value, size.value), 1)
  )

  def default = prop((size: SmallNatural) =>
    splitsFor(size.value, None).length must_== 1
  )

  def splitsFor(size: Int, mappers: Option[Int]): List[InputSplit] = {
    val conf = Data.conf
    load(conf, size)
    mappers.foreach(conf.setInt(DistributableMemoryInputFormat.Mappers, _))
    val format = new DistributableMemoryInputFormat
    format.getSplits(conf, 0).toList
  }

  def load(conf: JobConf, size: Int): List[Tuple] = {
    val tuples = Data.tuples(size)
    TupleMemoryInputFormat.storeTuples(conf, TUPLES_PROPERTY, tuples.asJava)
    tuples
  }
}
