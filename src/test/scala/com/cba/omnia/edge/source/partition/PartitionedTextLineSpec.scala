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

package com.cba.omnia.edge.source.partition

import scalaz._, Scalaz._

import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import com.cba.omnia.edge.test.ScaldingSpec

class PartitionedTextLineSpec extends ScaldingSpec { def is =s2"""

PartitionedTextLine
=================

  can write using PartitionedTextLine $write
"""

  type KV = (Int, String)

  lazy val data = List((1 -> "a"), (2 -> "b"), (1 -> "x"), (2 -> "y"))

  def slurp(dir: Path) = for {
    files <- Hdfs.files(dir, "part-*")
    all <- files.traverse(p => Hdfs.lines(p))
  } yield all.flatten

  def write =  new JobSpec {
    IterablePipe(data)
      .write(PartitionedTextLine[Int](testid, "%s"))
  } must runWith {
    val groups = data.groupBy({ case (k, v) => k })

    groups.foreach({ case (k, vs) =>
      val path = testpath.suffix("/" + k)

      Hdfs.exists(path) must beValue(true)

      slurp(path) must beValueLike { actual =>
        actual.size must_== vs.size
        actual.toSet must_== vs.map { case (k, v) => v }.toSet
      }
    })

    ok
  }
}

