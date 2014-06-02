package com.cba.omnia.edge
package test

import org.apache.hadoop.io.Writable
import java.io._

object Writables {
  def write[A <: Writable](value: A): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    value.write(output)
    bytes.toByteArray
  }

  def read[A <: Writable](data: Array[Byte], value: A): A = {
    val bytes = new ByteArrayInputStream(data)
    val input = new DataInputStream(bytes)
    value.readFields(input)
    value
  }
}
