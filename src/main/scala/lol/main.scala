package lol

import scalaz._
import Scalaz._

object Main {

  import lol.Mongo._
  import lol.Mongo.Implicits._
  import lol.Bson._
  import lol.Bson.Implicits._

  def example(conn: Connected) {
    val db   = Database("foo")
    val coll = Collection("bar", db)
    val docs = Seq(
         ("a" := "A")
        :: ("b" := "B")
      ,    ("a" := "A")
        :: ("c" := List[Value]("A", "B", "C"))
        :: ("d" := ("D" := "D")
                :: ("d" := "d"))
    )

    val query: Document = ("a" := "A") :: Nil

    val i = insert(docs)(coll)_
    val q = find(query)(coll)_
    val d = removeAll(query)(coll)_
    val e = getLastError()(db)_

    def exec(implicit c: Connected) {
      run(i)
      run(e) map (println)
      run(q) map (println)
      run(d)
      assume (run(q).docs.size == 0)
    }
    exec(conn)
  }

  def main(args: Array[String]) {
    bracket(
      { connect("localhost", 27017).get },
      disconnect,
      example
    )
  }

  def bracket[A,B,C](before: => A, after: A => B, thing: A => C): C = {
    val a = before
    try { thing(a) } finally { after(a) }
  }
}

