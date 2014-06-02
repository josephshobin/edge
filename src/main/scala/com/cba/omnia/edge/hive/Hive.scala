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
