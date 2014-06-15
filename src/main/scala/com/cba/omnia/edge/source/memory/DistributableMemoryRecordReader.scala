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
import com.twitter.maple.tap.TupleMemoryInputFormat
import com.twitter.maple.tap.TupleWrapper
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred._
import java.util.{List => JList}

class DistributableMemoryRecordReader(tuples: JList[Tuple], split: DistributableMemoryInputSplit) extends TupleMemoryInputFormat.TupleRecordReader(tuples) {
  var position = 0

  override def next(k: TupleWrapper, v: NullWritable) =
    position < split.size && {
      k.tuple = tuples.get(split.offset + position)
      position += 1
      true
    }

  override def getProgress: Float =
    if (split.size == 0) 1 else position.toFloat / split.size
}
