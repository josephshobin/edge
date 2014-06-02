package com.cba.omnia.edge
package source.psv

import com.twitter.scalding._

import cascading.tuple.Fields

object TypedPsv {
  def apply[T : Manifest : TupleConverter](paths: Seq[String]): TypedDelimited[T] = {
    val f = Dsl.intFields(0 until implicitly[TupleConverter[T]].arity)
    apply[T](paths, f)
  }

  def apply[T : Manifest : TupleConverter](path: String): TypedDelimited[T] =
    apply[T](Seq(path))

  def apply[T : Manifest : TupleConverter](path: String, f: Fields): TypedDelimited[T] =
    apply[T](Seq(path), f)

  def apply[T : Manifest : TupleConverter](paths: Seq[String], f: Fields): TypedDelimited[T] =
    new TypedDelimited[T](paths, f, false, false, "|")
}
