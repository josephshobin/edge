package com.cba.omnia.edge
package hive

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import com.cba.omnia.edge.hdfs._
import org.scalacheck._, Arbitrary._


class HiveSpec extends test.ScaldingSpec { def is = s2"""
HDFS Utility Operations on hive Managed Data
============================================

We should be able to:
  Get the min dataset within a partition          $min
  Get the max dataset within a partition          $max
  Get the min dataset invalid partition naming    $neg

Using the safe comparitor:
  Sort Mixed List correctly                       $negSort

"""

  def min = prop((p: Path) =>
    (Hdfs.mkdirs(p) >>
      Hdfs.mkdirs(p.suffix("/4")) >>
      Hdfs.mkdirs(p.suffix("/2")) >>
      Hdfs.mkdirs(p.suffix("/3")) >>
      Hdfs.mkdirs(p.suffix("/11")) >>
      Hive.minPartitionM(p) ) must beValueLike(_ must endWithPath(Some(p.suffix("/2")))))


  def max = prop((p: Path) =>
    (Hdfs.mkdirs(p) >>
      Hdfs.mkdirs(p.suffix("/1")) >>
      Hdfs.mkdirs(p.suffix("/2")) >>
      Hdfs.mkdirs(p.suffix("/3")) >>
      Hdfs.mkdirs(p.suffix("/12")) >>
      Hive.maxPartitionM(p) ) must beValueLike(_ must endWithPath(Some(p.suffix("/12")))))

  def neg = prop((p: Path) =>
    (Hdfs.mkdirs(p) >>
      Hdfs.mkdirs(p.suffix("/4")) >>
      Hdfs.mkdirs(p.suffix("/2")) >>
      Hdfs.mkdirs(p.suffix("/sdfsdf")) >>
      Hive.minPartitionM(p) ) must beValueLike(_ must endWithPath(Some(p.suffix("/2")))))

  val prefix = "/root"

  def negSort = List(s"$prefix/2",s"$prefix/asdfsdfs",s"$prefix/11a")
    .map(dir => new Path(dir))
    .sortWith(Hive.safeCompare(_ < _)_).head.toString mustEqual s"$prefix/2"
}
