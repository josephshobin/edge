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
import cascading.tap.SourceTap
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple.Tuple
import cascading.tuple.TupleEntryIterator
import cascading.tuple.Fields

import com.twitter.maple.tap.TupleWrapper

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer


/**
 * This is an implementation of an in memory source tap that can be
 * configurated to distribute explicitly on `mappers` number of mappers.
 *
 * This code is largely based on (and delegates to components of) the maple
 * implementation of a MemorySourceTap, see <https://github.com/Cascading/maple>,
 * the maple source is available under an EPL license.
 */
class DistributableMemorySourceTap(tuples: Buffer[Tuple], fields: Fields, mappers: Int, scheme: DistributableMemorySourceScheme)
    extends SourceTap[JobConf, RecordReader[TupleWrapper, NullWritable]](scheme) with Serializable {

  def this(tuples: Buffer[Tuple], fields: Fields, mappers: Int) =
    this(tuples, fields, mappers, DistributableMemorySourceScheme(tuples, fields, mappers))

  lazy val id =
    getScheme.asInstanceOf[DistributableMemorySourceScheme].getId

  override def getIdentifier: String =
    id

  override def resourceExists(conf: JobConf): Boolean =
    true

  override def equals(o: Any) =
    o.isInstanceOf[DistributableMemorySourceTap] &&
      o.asInstanceOf[DistributableMemorySourceTap].id == id

  override def hashCode =
    id.hashCode

  override def openForRead(flowProcess: FlowProcess[JobConf], input: RecordReader[TupleWrapper, NullWritable]): TupleEntryIterator =
    new HadoopTupleEntrySchemeIterator(flowProcess, this, input)

  override def getModifiedTime(conf: JobConf): Long =
    System.currentTimeMillis
}
