import com.twitter.scalding._, TDsl._
import com.twitter.scalding.typed.IterablePipe

import com.cba.omnia.edge.source.partition._

class Write(args: Args) extends Job(args) {
  val data = List(
    (("a", "x"), ("i", 1)),
    (("a", "y"), ("j", 2)),
    (("b", "z"), ("k", 3))
  )
  IterablePipe(data, flowDef, mode)
    .write(PartitionedCsv[(String, String), (String, Int)](
      args("out"),
      "col1=%s/col2=%s"
    ))
}

class Read(args: Args) extends Job(args) {
  PartitionedCsv[(String, String), (String, Int)](
      args("in"),
      "col1=%s/col2=%s"
    ).map(s => s.toString)
    .write(TypedCsv(args("out")))
}

class Write2(args: Args) extends Job(args) {
  val data = List(
    (("a", "x"), "i"),
    (("a", "y"), "j"),
    (("b", "z"), "k")
  )
  IterablePipe(data, flowDef, mode)
    .write(PartitionedTextLine[(String, String)](
      args("out"),
      "col1=%s/col2=%s"
    ))
}

class Read2(args: Args) extends Job(args) {
  PartitionedTextLine[(String, String)](
    args("in"),
    "col1=%s/col2=%s"
  ).map(s => s.toString)
    .write(TypedCsv(args("out")))
}
