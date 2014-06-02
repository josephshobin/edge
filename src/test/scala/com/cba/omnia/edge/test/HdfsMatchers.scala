package com.cba.omnia.edge
package test

import hdfs.{Hdfs, Result, Ok, Error}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalacheck._, Arbitrary._, Gen._
import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.execute.{Result => SpecResult}
import com.cba.omnia.edge.hdfs.Ok
import com.cba.omnia.edge.hdfs.Error

/**
 * This trait is designed to be mixed into a base Specification
 * class. It contains a set of matchers and arbitraries useful
 * for testing on HDFS.
 *
 * It also requires additional context to be mixed into that
 * base specification. Specifically it requires a hadoop
 * configuration object for accessing HDFS and and a unique test
 * identifier to isolate paths on HDFS.
 *
 * To use the matchers use the `must be{type}` form of assertion.
 */
trait HdfsMatchers { self: Spec with ConfigurationContext with UniqueContext =>

  def beResult[A](expected: Result[A]): Matcher[Hdfs[A]] =
    (h: Hdfs[A]) => h.run(conf) must_== expected

  def beResultLike[A](expected: Result[A] => SpecResult): Matcher[Hdfs[A]] =
    (h: Hdfs[A]) => expected(h.run(conf))

  def beValue[A](expected: A): Matcher[Hdfs[A]] =
    beResult(Result.ok(expected))

  //TODO(andersqu): Maybe figure out a better way of doing this. Hacky
  def endWithPath(t: => Option[Path]) = new Matcher[Option[Path]] {
    def apply[P <: Option[Path]](b: Expectable[P]) = {
      val a = t.getOrElse(new Path("")).toString
      result(b.value!= null && a!= null && b.value.getOrElse(new Path("")).toString.endsWith(a) ,
        b.description  + " ends with '" + a + "'",
        b.description  + " doesn't end with '" + a + "'",b)
    }
  }

  def beValueLike[A](expected: A => SpecResult): Matcher[Hdfs[A]] =
    beResultLike[A]({
      case Ok(v) =>
        expected(v)
      case Error(_) =>
        failure
    })

  implicit def PathArbitrary: Arbitrary[Path] =
    Arbitrary(for {
      n <- choose(1, 4)
      parts <- listOfN(n, identifier)
    } yield testpath.suffix(s"""/arbitrary/${Data.PathId.getAndIncrement}/${parts.mkString("/")}"""))
 }
