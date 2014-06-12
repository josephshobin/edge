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

import com.twitter.maple.tap.TupleMemoryInputFormat, TupleMemoryInputFormat.TUPLES_PROPERTY
import com.twitter.maple.tap.TupleWrapper
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred._
import scala.collection.JavaConverters._

class DistributableMemoryInputFormat extends TupleMemoryInputFormat {
  override def getSplits(conf: JobConf, n: Int): Array[InputSplit] = {
    val tuples = TupleMemoryInputFormat.retrieveTuples(conf, TUPLES_PROPERTY).asScala
    val length = tuples.length
    val mappers = math.max(math.min(conf.getInt(DistributableMemoryInputFormat.Mappers, 1), length), 1)
    val per = length / mappers
    (0 until mappers).map(chunk =>
      new DistributableMemoryInputSplit(Math.min(per, length - chunk * per), chunk * per)
    ).toArray
  }

  override def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter): RecordReader[TupleWrapper, NullWritable] = {
    val tuples = TupleMemoryInputFormat.retrieveTuples(conf, TUPLES_PROPERTY)
    new DistributableMemoryRecordReader(tuples, split.asInstanceOf[DistributableMemoryInputSplit])
  }
}

object DistributableMemoryInputFormat {
  val Mappers = "distributable.memory.inputformat.mappers"
}
