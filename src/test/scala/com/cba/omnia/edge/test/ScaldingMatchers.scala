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

import com.twitter.scalding._

import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration
import org.specs2.matcher.Matcher
import org.specs2.execute.{Result => SpecResult}

/**
 * This trait is designed to be mixed into a base Specification
 * class. It contains a set of matchers and for working with
 * scalding jobs (or part there of).
 *
 * This would be mised into a base specification with:
 * {{{
 *  abstract class Spec with ScaldingMatchers with ConfigurationContext
 * }}}
 *
 * To use the matchers use the `must run{type}` form of assertion.
 */
trait ScaldingMatchers { self: Spec with ConfigurationContext =>
  def getHadoopConfig = conf

  def run: Matcher[JobTest] =
    (j: JobTest) => { j.run.runHadoop; ok }

  def runInMemory: Matcher[JobTest] =
    (j: JobTest) => { j.run; ok }

  def runHadoop: Matcher[JobTest] =
    (j: JobTest) => { j.runHadoop; ok }

  class JobSpec extends Job(Args("--hdfs")) {
    override implicit def mode: Mode =
      com.twitter.scalding.Hdfs(false, getHadoopConfig)
  }

  lazy val testArgs = {
    val args = Args("--hdfs")
    Mode.putMode(com.twitter.scalding.Hdfs(false, getHadoopConfig), args)
  }

  def runWith(expectation: => SpecResult): Matcher[Job] = (job: Job) => {
    def start(j : Job, cnt : Int) {
      val successful = {
        j.validate
        j.run
      }
      j.clear
      if(successful)
        j.next match {
          case Some(nextj) => start(nextj, cnt + 1)
          case None => Unit
        }
      else
        sys.error(s"Job failed to run <${j.name}>")
    }

    job.mode must beAnInstanceOf[Hdfs]
    start(job, 0)
    expectation
  }
}
