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

package com.cba.omnia.edge.source.compressible

import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}

import cascading.scheme.Scheme
import cascading.scheme.hadoop.{TextLine, TextDelimited}
import cascading.tuple.Fields

import com.twitter.scalding._

// with thanks to https://github.com/morazow/WordCount-Compressed

/** A trait for defining compressible typed delimited source factories */
trait CompressibleTypedSeparatedFile extends TypedSeperatedFile {
  override def apply[T : Manifest : TupleConverter : TupleSetter](paths: Seq[String], f: Fields): FixedPathTypedDelimited[T] =
    new CompressibleTypedDelimited[T](paths, f, skipHeader, writeHeader, separator)
}

/** A factory for producing typed tab-separated sources which obey hadoop compression options */
object CompressibleTypedTsv extends CompressibleTypedSeparatedFile {
  val separator = "\t"
}

/** A factory for producing typed comma-separated sources which obey hadoop compression options */
object CompressibleTypedCsv extends CompressibleTypedSeparatedFile {
  val separator = ","
}

/** A factory for producing typed pipe-separated sources which obey hadoop compression options */
object CompressibleTypedPsv extends CompressibleTypedSeparatedFile {
  val separator = "|"
}

/** A factory for producing typed SOH-separated sources which obey hadoop compression options */
object CompressibleTypedOsv extends CompressibleTypedSeparatedFile {
  val separator = "\1"
}

/** A typed delimited source which obeys hadoop compression options */
class CompressibleTypedDelimited[T](p: Seq[String],
  override val fields:      Fields  = Fields.ALL,
  override val skipHeader:  Boolean = false,
  override val writeHeader: Boolean = false,
  override val separator:   String  = "\t"
)(
  implicit mf: Manifest[T],
  conv: TupleConverter[T],
  tset: TupleSetter[T]
) extends FixedPathTypedDelimited[T](p, fields, skipHeader, writeHeader, separator)(mf, conv, tset) {
  override def hdfsScheme = {
    val tmp = new TextDelimited(
      fields, TextLine.Compress.DEFAULT, skipHeader, writeHeader,
      separator, strict, quote, types, safe
    )
    // explicit conversion required as type parameters are not covariant
    tmp.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
  }
}
