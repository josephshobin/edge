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

import java.io.InputStream
import java.util.zip.GZIPInputStream

import scalaz._, Scalaz._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Args, Job, Mode, Hdfs => HdfsMode}
import com.twitter.scalding.typed.IterablePipe

import com.cba.omnia.edge.hdfs.Hdfs
import com.cba.omnia.edge.io.Streams
import com.cba.omnia.edge.test.ScaldingSpec

class CompressibleTypedDelimitedSpec extends ScaldingSpec { def is =s2"""

CompressibleTypedDelimited
==========================

  can write without compression $uncompressed
  can write with compression    $compressed
"""

  val dataPsv = Set("Foo|1", "Bar|2")

  def slurp(dir: Path, inflate: InputStream => InputStream): Hdfs[List[String]] = for {
    files      <- Hdfs.files(dir, "part-*")
    compressed <- files.traverse(Hdfs.open(_))
  } yield compressed.flatMap(in => Streams.read(inflate(in), "UTF-8").lines)

  def uncompressed = {
    val uncompressedConf = new Configuration
    val args = Mode.putMode(
      HdfsMode(false, uncompressedConf),
      Args(s"--hdfs --dest $testid")
    )
    new CopyJob(args) must runWith {
      slurp(testpath, in => in) must beValueLike {
        _.toSet must_== dataPsv
      }
    }
  }

  def compressed = {
    val compressedConf = new Configuration
    compressedConf.set("mapred.output.compress",          "true")
    compressedConf.set("mapred.output.compression.type",  "BLOCK")
    compressedConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    val args = Mode.putMode(
      HdfsMode(false, compressedConf),
      Args(s"--hdfs --dest $testid")
    )
    new CopyJob(args) must runWith {
      slurp(testpath, in => new GZIPInputStream(in)) must beValueLike {
        _.toSet must_== dataPsv
      }
    }
  }
}

class CopyJob(args: Args) extends Job(args) {
  val dest = args("dest")
  val data = List(("Foo", 1), ("Bar", 2))
  IterablePipe[(String, Int)](data, flowDef, mode)
    .write(CompressibleTypedPsv[(String, Int)](dest))
}
