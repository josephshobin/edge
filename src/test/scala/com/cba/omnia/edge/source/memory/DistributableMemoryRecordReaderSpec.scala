package com.cba.omnia.edge
package source.memory


import cascading.tuple.Tuple
import com.cba.omnia.edge.test._
import com.twitter.maple.tap.TupleWrapper
import java.io._
import org.apache.hadoop.io.NullWritable
import scala.collection.JavaConverters._


class DistributableMemoryRecordReaderSpec extends test.Spec { def is = s2"""
Distributable Memory Record Reader
==================================

Progress should:
  have accurate start                             $starting
  have accurate finish                            $finished
  have near enough parital progress               $partial

Iteration should:
  include all values, in order                    $iterate

"""

  def starting = prop((n: SmallNatural) => n.value > 0 ==> {
    val r = reader(n.value)
    r.getProgress == 0.0f
  })

  def finished = prop((n: SmallNatural) => {
    val r = reader(n.value)
    skip(r, n.value)
    r.getProgress must_== 1.0f
  })

  def partial = prop((n: SmallNatural, p: Percentage) => {
    val r = reader(n.value)
    skip(r, math.floor(p.of(n.value)).toInt + 1)
    r.getProgress must be_>= (p.percent / 100)
  })

  def iterate = prop((n: SmallNatural) => {
    val r = reader(n.value)
    (1 to n.value).foreach(v =>
      next(r).getInteger(0) must_== v
    )
  })

  def skip(reader: DistributableMemoryRecordReader, n: Int) =
    (1 to n).foreach(_ => next(reader))

  def next(reader: DistributableMemoryRecordReader) = {
    val wrapper = new TupleWrapper
    val writable = NullWritable.get
    reader.next(wrapper, writable)
    wrapper.tuple
  }

  def reader(size: Int) = {
    val tuples = Data.tuples(size).asJava
    val split = new DistributableMemoryInputSplit(size, 0)
    val reader = new DistributableMemoryRecordReader(tuples, split)
    reader
  }
}
