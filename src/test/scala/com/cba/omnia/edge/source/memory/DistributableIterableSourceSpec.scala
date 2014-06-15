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
package source.memory

import com.twitter.scalding._
import com.twitter.scalding.TDsl._

object DistributableIterableSourceSpec extends test.ScaldingSpec { def is = s2"""
Distributable Iterable Source
=============================

Source should:
  feed values to job                              $job

"""

  type KV = (Int, String)

  val data = List((1 -> "a"), (2 -> "b"), (3 -> "c"))

  class DistributableIterableJob(args: Args) extends Job(args) {
    TypedPipe.from[KV](DistributableIterableSource(data, 1), '*)
      .write(TypedTsv[KV]("output"))
  }

  def job =
    JobTest(classOf[DistributableIterableJob].getName)
      .sink[KV](TypedTsv[KV]("output"))(output =>
         output.toSet must_== data.toSet
      ) must run
}
