package com.cba.omnia.edge
package test

import com.twitter.scalding._

import java.io.File

import org.specs2._
import org.specs2.matcher._
import org.specs2.specification.{Fragments, Step, After}

/**
 * A base specification class for scalding/hdfs related testing.
 *
 * This specification will provide access to a unique path, via:
 * {{{
 *  testid     // for string form
 *  testpath   // for hdfs path form
 *  testfile   // for file form
 * }}}
 *
 * Arbitrary paths for use in properties are also provided and
 * will be generated under the unique test id.
 *
 * All files created will be cleaned up on completion of the
 * test.
 *
 * This base specification also include matchers for hdfs operations
 * and scalding jobs.
 */
abstract class ScaldingSpec extends Spec
  with ScaldingMatchers
  with HdfsMatchers
  with UniqueContext
  with ConfigurationContext
  with specification.AfterExample {

  def after =
    clean(testfile)

  override def map(fs: => Fragments) =
    isolated ^ fs

  def clean(dir: File): Unit = {
    if (dir.isDirectory)
      dir.listFiles match {
        case null => ()
        case fs   => fs.foreach(clean)
      }
    dir.delete()
  }
}
