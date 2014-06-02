package com.cba.omnia.edge
package codec

import org.scalacheck._, Arbitrary.arbitrary

import Codec._
import Codec.auto._

object CodecSpec extends test.Spec { def is = s2"""
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
