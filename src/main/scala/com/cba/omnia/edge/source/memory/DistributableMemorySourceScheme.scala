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


import cascading.flow.FlowProcess
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.Fields

import com.twitter.maple.tap.MemorySourceTap
import com.twitter.maple.tap.TupleMemoryInputFormat, TupleMemoryInputFormat.TUPLES_PROPERTY
import com.twitter.maple.tap.TupleWrapper

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred._

import java.util.{List => JList}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer


class DistributableMemorySourceScheme(tuples: JList[Tuple], fields: Fields, id: String, mappers: Int)
    extends MemorySourceTap.MemorySourceScheme(tuples, fields, id) {

  override def sourceConfInit(
    flowProcess: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[TupleWrapper, NullWritable], Void],
    conf: JobConf
  ) {
    FileInputFormat.setInputPaths(conf, id)
    conf.setInt(DistributableMemoryInputFormat.Mappers, mappers)
    conf.setInputFormat(classOf[DistributableMemoryInputFormat])
    TupleMemoryInputFormat.storeTuples(conf, TUPLES_PROPERTY, tuples)
  }
}

object DistributableMemorySourceScheme {
  def apply(tuples: Buffer[Tuple], fields: Fields, mappers: Int) =
    new DistributableMemorySourceScheme(tuples.asJava, fields, "/" + UUID.randomUUID.toString, mappers)
}
