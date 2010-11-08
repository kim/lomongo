package lol

import scalaz._
import Scalaz._

object Main {

  import lol.Mongo._
  import lol.Bson._
  import lol.Bson.Implicits._
  import lol.Protocol._

  def main(args: Array[String]) {
    val conn = connect("localhost", 27017)
    val db   = Database("foo")
    val coll = Collection("bar", db)

    def run[T](fn: Collection => (Connected => T)): T =
      fn(coll)(conn.get)

    val docs = Seq(
        ("a" =: "A") :: ("b" =: "B")
      , ("a" =: "A") :: ("c" =: "C") :: ("d" =: "D")
    )

    val i = insert(docs)_
    val q = find(where(("a" =: "A") :: Nil))_
    val d = removeAll(("a" =: "A") :: Nil)_

    run(i)
    run(q) map (println)
    run(d)
    run(q) map (println)

    conn map (disconnect)
  }
}

