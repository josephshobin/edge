package com.cba.omnia.edge
package test

import cascading.tuple.Tuple
import com.cba.omnia.edge.hdfs._
import org.apache.hadoop.mapred._
import org.scalacheck._, Arbitrary._, Gen._
import scalaz.\&/._
import scalaz.scalacheck.ScalazProperties._
import scalaz.scalacheck.ScalazArbitrary._

object Data {
  lazy val PathId = new java.util.concurrent.atomic.AtomicInteger(0)

  implicit def ResultAribtary[A: Arbitrary]: Arbitrary[Result[A]] =
    Arbitrary(arbitrary[Either[These[String, Throwable], A]] map {
      case Left(v)  => Error(v)
      case Right(v) => Ok(v)
    })

  def tuples(size: Int): List[Tuple] =
    (1 to size).map(v => {
      val tuple = new Tuple
      tuple.add(v)
      tuple
    }).toList

  def conf: JobConf = {
    val conf = new JobConf
    conf.set("io.serializations",  List(
      "org.apache.hadoop.io.serializer.WritableSerialization",
      "cascading.tuple.hadoop.TupleSerialization"
    ).mkString(","))
    conf
  }

  def order(n: Int, m: Int) =
    (math.min(n, m), math.max(n, m))
}


case class Identifier(value: String)

object Identifier {
  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(identifier map (Identifier.apply))
}


case class Percentage(percent: Float) {
  def of(n: Int) =
    n * percent
}

object Percentage {
  implicit def PercentageArbitrary: Arbitrary[Percentage] =
    Arbitrary(arbitrary[Float] map (v => Percentage((math.abs(v) % 100) / 100)))
}


case class Natural(value: Int)

object Natural {
  implicit def NaturalArbitrary: Arbitrary[Natural] =
    Arbitrary(arbitrary[Int] map (v => Natural(math.abs(v))))
}


case class SmallNatural(value: Int)

object SmallNatural {
  implicit def SmallNaturalArbitrary: Arbitrary[SmallNatural] =
    Arbitrary(arbitrary[Natural] map (v => SmallNatural(v.value % 1024)))
}
