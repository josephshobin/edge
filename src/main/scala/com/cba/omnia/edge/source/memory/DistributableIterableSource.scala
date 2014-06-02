package com.cba.omnia.edge
package source.memory

import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.Fields
import cascading.scheme.NullScheme

import com.twitter.scalding._

import java.io.{InputStream,OutputStream}

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import scala.collection.JavaConverters._

/**
 * This is an implementation of an iterable backed scalding source.
 *
 * This works in much the same way as does the standard IterableSource
 * but we can distribute the contents across a fixed number of mappers.
 * This is useful for explosive map jobs that use small inputs to
 * produce largs outputs.
 */
case class DistributableIterableSource[T](@transient iter: Iterable[T], mappers: Int, inFields: Fields = Fields.NONE)(
    implicit set: TupleSetter[T], conv: TupleConverter[T]) extends Source with Mappable[T] {

  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](conv)

  def fields =
    if (inFields.isNone && set.arity > 0)
      Dsl.intFields(0 until set.arity)
    else
      inFields

  def data =
    iter.map(set(_))

  def buffer =
    data.toBuffer

  def tap: Tap[_,_,_] =
    new DistributableMemorySourceTap(buffer, fields, mappers)

  def memory: Tap[_,_,_] =
    new MemoryTap[InputStream, OutputStream](new NullScheme(fields, fields), buffer)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_,_,_] =
    if (readOrWrite == Write)
      sys.error("Error using read only source.")
    else
      mode match {
        case Hdfs(_, _) => tap
        case HadoopTest(_,_) => tap
        case Local(_) => memory
        case Test(_) => memory
        case _ => sys.error(s"Unknown mode <$mode>")
      }
}
