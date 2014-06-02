package com.cba.omnia.edge
package source.template

import com.twitter.scalding._
import com.twitter.scalding.TDsl._

import org.specs2._
import org.specs2.execute._
import org.specs2.matcher._
import org.apache.hadoop.fs.Path

import com.cba.omnia.edge.hdfs.Hdfs
import com.cba.omnia.edge.hdfs.{Result, Ok, Error}
import com.cba.omnia.edge.hdfs.HdfsString._
import com.cba.omnia.edge.source.memory._

import scalaz._, Scalaz._

class TemplateSourceSpec extends test.ScaldingSpec { def is = s2"""
Template Source
===============
Template Delimited:
  bin writes with fields api                      $fields

"""

  type KV = (Int, String)

  lazy val data = List((1 -> "a"), (2 -> "b"), (1 -> "x"), (2 -> "y"))

  def slurp(dir: Path) = for {
    files <- Hdfs.files(dir, "part-*")
    all <- files.traverse(p => Hdfs.lines(p))
  } yield all.flatten

  def fields =  new JobSpec {

    IterableSource(data, ('k, 'v))
      .read
      .write(TemplateCsv(testid, "%s", 'k))

  } must runWith {
    val groups = data.groupBy({ case (k, v) => k })

    groups.foreach({ case (k, vs) =>
      val path = testpath.suffix("/" + k)

      Hdfs.exists(path) must beValue(true)

      slurp(path) must beValueLike { actual =>
        actual.size must_== vs.size
        actual.toSet must_== vs.map({ case (k, v) => k + "," + v }).toSet
      }
    })

    ok
  }

}
