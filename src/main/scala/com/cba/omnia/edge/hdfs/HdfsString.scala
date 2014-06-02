package com.cba.omnia.edge
package hdfs

/** Hdfs specific string methods. */
case class HdfsString(value: String) {
  def toPath =
    Hdfs.path(value)
}

object HdfsString extends HdfsStrings

trait HdfsStrings {
  implicit def StringToHdfsString(s: String) =
    HdfsString(s)
}
