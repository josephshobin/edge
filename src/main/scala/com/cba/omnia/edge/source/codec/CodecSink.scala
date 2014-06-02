package com.cba.omnia.edge
package source.codec

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.cba.omnia.edge.codec._
import com.twitter.scalding._, typed.TypedSink, Dsl._, TDsl._
import scalaz._, Scalaz._


object CodecSink extends FieldConversions {
  def apply[A: Codec](path: String, delimiter: String = "|"): TypedSink[A] =
    new TypedSink[A] {
      lazy val source = TextLine(path)
      def setter[U <: A] =
        TupleSetter.asSubSetter[A, U](
          new TupleSetter[A] {
            def arity: Int = 1
            def apply(a: A) =
              implicitly[TupleSetter[String]].apply(Codec.encode(a).mkString(delimiter))
          })
      def writeFrom(pipe: Pipe)(implicit flow: FlowDef, mode: Mode): Pipe =
        source.writeFrom(pipe)
    }
}
