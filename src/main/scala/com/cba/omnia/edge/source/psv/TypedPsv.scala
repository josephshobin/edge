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
package source.psv

import com.twitter.scalding.{TypedPsv => STypedPsv, _}

import cascading.tuple.Fields

object TypedPsv {
  @deprecated("Use TypedPsv in Scalding directly")
  def apply[T : Manifest : TupleConverter : TupleSetter](paths: Seq[String]): FixedPathTypedDelimited[T] = {
    val f = Dsl.intFields(0 until implicitly[TupleConverter[T]].arity)
    apply[T](paths, f)
  }

  @deprecated("Use TypedPsv in Scalding directly")
  def apply[T : Manifest : TupleConverter : TupleSetter](path: String): FixedPathTypedDelimited[T] =
    apply[T](Seq(path))

  @deprecated("Use TypedPsv in Scalding directly")
  def apply[T : Manifest : TupleConverter : TupleSetter](path: String, f: Fields): FixedPathTypedDelimited[T] =
    apply[T](Seq(path), f)

  @deprecated("Use TypedPsv in Scalding directly")
  def apply[T : Manifest : TupleConverter : TupleSetter](paths: Seq[String], f: Fields): FixedPathTypedDelimited[T] =
    STypedPsv[T](paths, f)
}
