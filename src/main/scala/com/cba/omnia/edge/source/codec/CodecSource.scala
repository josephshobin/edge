package com.cba.omnia.edge
package source.codec

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.cba.omnia.edge.codec._
import com.cba.omnia.edge.pipe._
import com.twitter.scalding._, typed.TypedSink, Dsl._, TDsl._
import scalaz._, Scalaz._


object CodecSource extends FieldConversions {
  // FIX proper delimited split, not regex based.
  def apply[A: Codec](path: String, errors: String, delimiter: String = "\\|")(implicit flow: FlowDef, mode: Mode): TypedPipe[A] =
    Errors.handle(TypedPipe.from[String](TextLine(path).read, 'line)
      .map(s => Codec.decode(s.split(delimiter, -1).toList)),  TextLine(errors))
}
