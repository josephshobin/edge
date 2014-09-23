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
package test

import cascading.tuple.Tuple

import org.apache.hadoop.mapred._

import org.scalacheck._, Arbitrary._, Gen._

import scalaz.\&/._
import scalaz.scalacheck.ScalazProperties._
import scalaz.scalacheck.ScalazArbitrary._

object Data {
  lazy val PathId = new java.util.concurrent.atomic.AtomicInteger(0)

  def tuples(size: Int): List[Tuple] =
    (1 to size).map(v => {
      val tuple = new Tuple
      tuple.add(v)
      tuple
    }).toList

  def conf: JobConf = {
    val conf = new JobConf
    conf.set("io.serializations",  List(
      "org.apache.hadoop.io.serializer.WritableSerialization",
      "cascading.tuple.hadoop.TupleSerialization"
    ).mkString(","))
    conf
  }
}

case class Percentage(percent: Float) {
  def of(n: Int) =
    n * percent
}

object Percentage {
  implicit def PercentageArbitrary: Arbitrary[Percentage] =
    Arbitrary(arbitrary[Float] map (v => Percentage((math.abs(v) % 100) / 100)))
}


case class Natural(value: Int)

object Natural {
  implicit def NaturalArbitrary: Arbitrary[Natural] =
    Arbitrary(arbitrary[Int] map (v => Natural(math.abs(v))))
}


case class SmallNatural(value: Int)

object SmallNatural {
  implicit def SmallNaturalArbitrary: Arbitrary[SmallNatural] =
    Arbitrary(arbitrary[Natural] map (v => SmallNatural(v.value % 1024)))
}
