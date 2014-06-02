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
