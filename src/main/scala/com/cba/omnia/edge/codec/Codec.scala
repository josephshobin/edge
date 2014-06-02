package com.cba.omnia.edge
package codec

import scala.util.Try
import scala.util.{Success => TrySuccess}
import scala.util.{Failure => TryFailure}
import scalaz._, Scalaz._
import shapeless._


case class Codec[A](
  to: A => List[String],
  from: List[String] => String \/ (List[String], A)
) {
  def bimap[B](f: A => B, g: B => A): Codec[B] =
    Codec[B](g andThen to, x => from(x).map(_.map(f)))
}

object Codec extends TypeClassCompanion[Codec] {
  def decode[A](l: List[String])(implicit A: Codec[A]): String \/ A =
    A.from(l).flatMap({
      case (Nil, a) =>
        a.right[String]
      case (xs, a) =>
        s"""Unexpected fields in data: [${xs.mkString(", ")}]""".left[A]
    })

  def encode[A](a: A)(implicit A: Codec[A]): List[String] =
    A.to(a)

  implicit def IntCodec: Codec[Int] =
    TryCodec[Int](_.toString, _.toInt)

  implicit def LongCodec: Codec[Long] =
    TryCodec[Long](_.toString, _.toLong)

  implicit def DoubleCodec: Codec[Double] =
    TryCodec[Double](_.toString, _.toDouble)

  implicit def BooleanCodec: Codec[Boolean] =
    TryCodec[Boolean](_.toString, _.toBoolean)

  implicit def StringCodec: Codec[String] =
    ElementCodec[String](identity, _.right[String])

  implicit def CodecTypeClass: ProductTypeClass[Codec] = new ProductTypeClass[Codec] {
    def emptyProduct =
      ConstCodec(HNil)

    def product[A, T <: HList](A: Codec[A], T: Codec[T]) =
      Codec(a => A.to(a.head) ++ T.to(a.tail), l => for {
        a <- A.from(l)
        (m, v) = a
        t <- T.from(m)
        (n, u) = t
      } yield (n, v :: u))

    def project[F, G](instance: => Codec[G], to : F => G, from : G => F) =
      instance.bimap(from, to)
  }

  def ElementCodec[A](to: A => String, from: String => String \/ A) = {
    import scala.collection.immutable._

    Codec[A](x => List(to(x)), l => l match {
      case x :: xs =>
        from(x).map(a => xs -> a)
      case _ =>
        "Unexpected EOF.".left[(List[String], A)]
    })
  }

  def ConstCodec[A](v: => A) =
    Codec[A](_ => List(), xs => (xs, v).right[String])

  def TryCodec[A](to: A => String, unsafe: String => A) =
    ElementCodec[A](_.toString, x =>
      Try(unsafe(x)) match {
        case TrySuccess(v) => v.right[String]
        case TryFailure(e) => e.getMessage.left[A]
      })
}
