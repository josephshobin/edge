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
