package com.cba.omnia.edge
package hdfs

import test.Data._

import scalaz._, Scalaz._, \&/._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalazProperties._


class ResultSpec extends test.Spec { def is = s2"""
Result
======

Result should:
  obey monad laws                                 $laws
  toDisjuntion should roundtrip                   $toDisjunctionRoundtrip
  toEither should roundtrip                       $toEitherRoundtrip
  toOption should always be Some for Ok           $toOptionOk
  toOption should always be None for Error        $toOptionError
  getOrElse should always return value for Ok     $getOrElseOk
  getOrElse should always return else for Error   $getOrElseError
  ||| is alias for `or`                           $orAlias
  or returns first Ok                             $orFirstOk
  or returns first Error                          $orFirstError
  setMessage on Ok is noop                        $setMessageOk
  setMessage on Error always sets message         $setMessageError
  setMessage maintains any Throwable              $setMessageMaintainsThrowable

"""

  def laws =
    monad.laws[Result]

  def toDisjunctionRoundtrip = prop((x: These[String, Throwable] \/ Int) =>
    x.fold(Result.these, Result.ok).toDisjunction must_== x)

  def toEitherRoundtrip = prop((x: Either[These[String, Throwable], Int]) =>
    x.fold(Result.these, Result.ok).toEither must_== x)

  def toEither = prop((x: Int) =>
    Result.ok(x).toOption must beSome(x))

  def toOptionOk = prop((x: Int) =>
    Result.ok(x).toOption must beSome(x))

  def toOptionError = prop((x: String) =>
    Result.fail(x).toOption must beNone)

  def getOrElseOk = prop((x: Int, y: Int) =>
    Result.ok(x).getOrElse(y) must_== x)

  def getOrElseError = prop((x: String, y: Int) =>
    Result.fail(x).getOrElse(y) must_== y)

  def orAlias = prop((x: Result[Int], y: Result[Int]) =>
    (x ||| y) must_== (x or y))

  def orFirstOk = prop((x: Int, y: Result[Int]) =>
    (Result.ok(x) ||| y) must_== Result.ok(x))

  def orFirstError = prop((x: String, y: Result[Int]) =>
    (Result.fail(x) ||| y) must_== y)

  def setMessageOk = prop((x: Int, message: String) =>
    Result.ok(x).setMessage(message) must_== Result.ok(x))

  def setMessageError = prop((x: These[String, Throwable], message: String) =>
    Result.these(x).setMessage(message).toError.flatMap(_.a) must beSome(message))

  def setMessageMaintainsThrowable = prop((x: These[String, Throwable], message: String) =>
    Result.these(x).setMessage(message).toError.flatMap(_.b) must_== x.b)

  /** Note this is not general purpose, specific to testing laws. */

  implicit def ResultEqual[A]: Equal[Result[A]] =
    Equal.equalA[Result[A]]
}
