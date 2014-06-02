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
