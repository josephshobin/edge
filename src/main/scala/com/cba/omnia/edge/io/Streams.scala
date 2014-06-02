package com.cba.omnia.edge
package io

import java.io.{InputStream, OutputStream, ByteArrayOutputStream, PrintStream}

object Streams {
  val BufferSize = 4096

  def read(in: InputStream, encoding: String = "UTF-8") = {
    val buffer = Array.ofDim[Byte](BufferSize)
    val out = new ByteArrayOutputStream
    var size = 0
    while ({ size = in.read(buffer); size != -1 })
      out.write(buffer, 0, size)
    new String(out.toByteArray, encoding)
  }

  def write(out: OutputStream, content: String, encoding: String = "UTF-8") = {
    val writer = new PrintStream(out, false, encoding);
    try writer.print(content)
    finally writer.close
  }
}
