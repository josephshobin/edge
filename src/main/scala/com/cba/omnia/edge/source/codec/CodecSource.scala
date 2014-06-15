//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

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
