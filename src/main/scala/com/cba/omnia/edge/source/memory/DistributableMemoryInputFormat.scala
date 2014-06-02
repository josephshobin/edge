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
