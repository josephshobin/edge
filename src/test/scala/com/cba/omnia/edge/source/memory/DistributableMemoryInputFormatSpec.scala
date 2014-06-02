package com.cba.omnia.edge
package source.memory

import cascading.tuple.Tuple
import com.cba.omnia.edge.test._
import com.twitter.maple.tap.TupleMemoryInputFormat, TupleMemoryInputFormat.TUPLES_PROPERTY
import org.apache.hadoop.mapred._
import scala.collection.JavaConverters._

class DistributableMemoryInputFormatSpec extends test.Spec { def is = s2"""
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
