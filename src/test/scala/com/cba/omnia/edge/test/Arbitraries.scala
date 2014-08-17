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

import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._, \&/._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._

import org.scalacheck._, Arbitrary._

import com.cba.omnia.edge.hdfs.{Hdfs, Result}
import com.cba.omnia.edge.test.Data._

/**
  * Arbitraries for edge types
  *
  * Does not yet replace arbitrary instances defined in other objects
  */
object Arbitraries {
  implicit def HdfsIntArbitrary: Arbitrary[Hdfs[Int]] =
    Arbitrary(arbitrary[Configuration => Result[Int]] map (Hdfs(_)))

  implicit def HdfsBooleanArbitrary: Arbitrary[Hdfs[Boolean]] =
    Arbitrary(arbitrary[Configuration => Result[Boolean]] map (Hdfs(_)))
}
