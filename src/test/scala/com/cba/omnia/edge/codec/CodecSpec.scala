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
package codec

import org.scalacheck._, Arbitrary.arbitrary

import au.com.cba.omnia.permafrost.test.Spec

import Codec._
import Codec.auto._

object CodecSpec extends Spec { def is = s2"""
Codec Spec
==========

Primitives:
  Int                              ${symmetric[Int]}
  Long                             ${symmetric[Long]}
  Double                           ${symmetric[Double]}
  Boolean                          ${symmetric[Boolean]}
  String                           ${symmetric[String]}

Automated:
  Basic                            ${symmetric[Record]}
  Nested                           ${symmetric[Nested]}

"""

  def symmetric[A: Arbitrary: Codec] = prop((a: A) =>
    Codec.decode[A](Codec.encode[A](a)).toEither must beRight(a))

  case class Record(s: String, n: Int, b: Boolean)
  case class Nested(r: Record, l: Long)

  implicit def RecordArbitrary: Arbitrary[Record] =
    Arbitrary(arbitrary[(String, Int, Boolean)].map(Record.tupled))

  implicit def NestedArbitrary: Arbitrary[Nested] =
    Arbitrary(arbitrary[(Record, Long)].map(Nested.tupled))

}
