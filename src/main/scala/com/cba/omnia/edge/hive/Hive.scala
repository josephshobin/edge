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

package com.cba.omnia.edge.hive

import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream, FSDataOutputStream}
import com.cba.omnia.edge.hdfs.Hdfs
import scalaz._, Scalaz._
import scalaz.std.{list => l}

object Hive {

  implicit def pathToString(p:Path):String = p.toString.split(Path.SEPARATOR).last

  def safeStringToInt(str: String): Option[Int] = {
    import scala.util.control.Exception._
    catching(classOf[NumberFormatException]) opt str.toInt
  }

  def safeCompare(f: (Int,Int) => Boolean)(a:Path,b:Path) = (safeStringToInt(a),safeStringToInt(b)) match{
    case (Some(x),Some(y)) => f(x,y)
    case _ => false
  }

  def partition(path: Path, f: (Path,Path) => Boolean): Hdfs[Option[Path]] = for{
    fs <- Hdfs.filesystem
    files <- Hdfs.value { fs.globStatus(new Path(path,"*")).toList.map(_.getPath) }
    dirs <- files.filterM(Hdfs.isDirectory)
  } yield {
    dirs.sortWith(f).headOption
  }


  def minPartitionM(path: Path): Hdfs[Option[Path]] = partition(path,safeCompare(_ < _)_)
  def maxPartitionM(path: Path): Hdfs[Option[Path]] = partition(path,safeCompare(_ > _)_)

}
