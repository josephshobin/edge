package com.cba.omnia.edge
package pipe

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding._, typed.TypedSink, Dsl._, TDsl._
import scalaz._, Scalaz._


object Errors extends FieldConversions {

  /*
   * This is totally unacceptable but is the only mechanism by which we have
   * been able to construct error handling for validation on sources that can
   * be written to file and doesn't involve doing multiple passes on the
   * source data.
   *
   * It is also _dangerous_ in that you can not add multiple traps. If you
   * do, you will receive a _runtime_ error (please do not swallow this,
   * fix your program so it only has one trap).
   *
   * The process is basically that we give up on types and hack away:
   *   - Convert back to a regular pipe.
   *   - Add a trap on the regular pipe, for routing all failures to an error source.
   *   - Convert back to a typed pipe.
   *   - Go into untyped land by stripping off the "left" data constructor (this
   *     means we end up with a TypedPipe[Any] really that will either have raw
   *     failures, or success in a "right" data constructor. We do this so when
   *     errors are written out they are raw, and don't have the \/-() constructor.
   *   - Make the conversion strict via a call to fork.
   *   - Force errors into error pipe, by:
   *     - Doing a runtime match on rights, and returning the raw value.
   *     - For all other cases (i.e. our errors), throw an exception to trigger
   *       the trap. This should neatly write out the error because of our
   *       previous hack to strip off the left constructor.
   */
  def handle[A](p: TypedPipe[String \/ A], errors: Source)(implicit flow: FlowDef, mode: Mode): TypedPipe[A] =
    p.toPipe('value)
      .addTrap(errors)
      .toTypedPipe[String \/ A]('value)
      .flatMap({
        case     -\/(error) => List(error)
        case v @ \/-(value) => List(v)
      })
      .fork
      .map({
        case \/-(value) => value.asInstanceOf[A]
        case _          => sys.error("trap")
      })
}
