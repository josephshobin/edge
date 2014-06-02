package com.cba.omnia.edge
package source.memory

import test.Writables._

class DistributableMemoryInputSplitSpec extends test.Spec { def is = s2"""
Distributable Memory Input Split
================================

Serialization should
  be symmetric                                    $symmetric

"""

  def symmetric = prop((size: Int, offset: Int) => {
    val split = new DistributableMemoryInputSplit(size, offset)
    val result = read(write(split), new DistributableMemoryInputSplit())
    (result.size, result.offset) must_== ((size, offset))
  })
}
