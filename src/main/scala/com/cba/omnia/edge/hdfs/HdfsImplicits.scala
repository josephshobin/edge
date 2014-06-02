package com.cba.omnia.edge
package hdfs

import scalaz._, Scalaz._

trait HdfsImplicits {

  implicit def validation2Hdfs[A](v: Validation[String, A]): HdfsValidation[A] =
    HdfsValidation(v)
}

object HdfsImplicits extends HdfsImplicits

case class HdfsValidation[A](v: Validation[String, A]) {

  def toHdfs = v match {
    case Failure(e) => Hdfs.fail(e)
    case Success(m) => Hdfs.value(m)
  }
}
