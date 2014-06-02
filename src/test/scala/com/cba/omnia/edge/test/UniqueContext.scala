package com.cba.omnia.edge
package test

import com.cba.omnia.edge.hdfs.HdfsString._
import java.io.File

trait UniqueContext {
  lazy val uniqueId = s"${UniqueContext.jvm}.${UniqueContext.Ids.getAndIncrement}"

  def testid =
    s"target/hdfs/$uniqueId"

  def testpath =
    testid.toPath

  def testfile =
    new File(testid)

}

object UniqueContext {
  val Ids = new java.util.concurrent.atomic.AtomicInteger(0)

  def jvm =
    new java.rmi.dgc.VMID().toString.replace(':', '.')
}
