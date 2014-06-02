package com.cba.omnia.edge
package source.memory

import java.io.{DataInput,DataOutput}
import org.apache.hadoop.mapred._

class DistributableMemoryInputSplit(var size: Int, var offset: Int) extends InputSplit {
  def this() = this(0, 0)
  def getLength(): Long = size
  def getLocations(): Array[String] = Array()
  def write(out: DataOutput): Unit = {
    out.writeInt(size)
    out.writeInt(offset)
  }
  def readFields(in: DataInput): Unit = {
    size = in.readInt
    offset = in.readInt
  }
}
