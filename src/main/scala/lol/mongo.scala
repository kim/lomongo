package lol

import scalaz._
import Scalaz._
import collection.mutable.ListBuffer

object Mongo {

  import Bson._
  import Bson.Implicits._
  import Protocol._
  import IO.{ Connected, ReplyPromise }


  type WriteResult = Document
  type Error = String
  type Command = Document
  type Connected = IO.Connected


  case class Database(name: String)
  case class Collection(name: String, db: Database)

  def connect(host: String, port: Int): Option[Connected] =
    IO.connect(host, port)
  def disconnect(conn: Connected) {
    IO.disconnect()(conn)
  }

  def run[T](fn: Connected => T)(implicit c: Connected) = fn(c)

  private def run(r: Request)(c: Connected): ReplyPromise = IO.send(r)(c)
  private def run(n: Notice)(c: Connected) = IO.send(n)(c)


  implicit def toNamespace(c: Collection): Namespace =
    new Namespace(c.db.name + "." + c.name)
  implicit def toNamespace(db: Database): Namespace =
    new Namespace(db.name)

  case class Cursor(id: CursorId
                  , coll: Collection
                  , offset: Int
                  , limit: Int
                  , docs: Seq[Document]
                  , conn: Connected) {

    def foreach[U](f: Document => U) {
      docs.foreach(f)
      if (!docs.empty && id != 0) {
        if (limit == 0 || (limit - offset) > 0) {
          run(GetMore(coll, 0, id))(conn) map (reply =>
            Cursor(reply.cursorId
                 , coll
                 , reply.startingFrom
                 , limit
                 , reply.docs
                 , conn)) foreach (f)
        } else {
          run(KillCursors(Seq(id)))(conn)
        }
      }
    }

    def map[T](f: Document => T): Seq[T] = {
      val buf = ListBuffer[T]()
      foreach(d => buf += f(d))
      List() ++ buf
    }
  }


  case class UserQuery(s: Selector = Selector()
                     , p: Option[Projection] = None
                     , skip: Int = 0
                     , limit: Int = 0) {
    def select(fs: List[String]): UserQuery = copy(s, p >>= (proj =>
      Some(Projection(proj.p ++ fs.map(_ := 1)))))
    def where(c: Document): UserQuery  = copy(s.where(c), p)
    def orderBy(o: Map[String,Boolean]): UserQuery  = copy(s.sort(o), p)
    def skip(i: Int): UserQuery = copy(s, p, i, limit)
    def limit(i: Int): UserQuery = copy(s, p, skip, i)
  }

  def where(c: Document): UserQuery = UserQuery(Selector(c))

  case class Projection(p: Document = Document.empty)
  case class Selector(q: Document = Document.empty) {
    def where(c: Document) = copy(("$query" := c) :: q)

    def sort(by: Map[String,Boolean]) = {
      val orderby = by.foldLeft(Document.empty)((d,kv) => {
        val (field,asc) = kv
        (field := (if (asc) 1 else -1)) :: d
      })
      copy(("$orderby" := orderby) :: q)
    }

    def returnKey = copy(("$returnKey" := 1) :: q)
    def maxScan(i: Int) = copy(("$maxScan" := i) :: q)
    def explain = copy(("$explain" := true) :: q)
    def snapshot = copy(("$snapshot" := true) :: q)
    def hint(h: Document) = copy(("$hint" := h) :: q)
    def min(m: Document) = copy(("$min" := m) :: q)
    def max(m: Document) = copy(("$max" := m) :: q)
    def showDiskLoc = copy(("$showDiskLoc" := true) :: q)
  }

  // functions on collection
  def find(q: UserQuery, opts: Seq[QueryOption] = Seq())
          (coll: Collection)(conn: Connected): Cursor =
  {
    val query = Query(ns   = coll
                    , opts = opts
                    , skip = q.skip
                    , batchSize = 0 // TODO...
                    , selector  = q.s.q
                    , projector = q.p map (_.p))
    run(query)(conn) map (reply => {
      Cursor(reply.cursorId
           , coll
           , reply.startingFrom
           , q.limit
           , reply.docs
           , conn)
    })
  }

  def findOne(q: UserQuery, opts: Seq[QueryOption] = Seq())
             (coll: Collection)(conn: Connected): Option[Document] = {
    val query = Query(ns = coll
                    , opts = opts
                    , skip = q.skip
                    , batchSize = 1
                    , selector  = q.s.q
                    , projector = q.p map (_.p))
    run(query)(conn) map (_.docs.headOption)
  }


  // write operations
  // NOTE: leaking abstraction of "safe mode" omitted, use something like
  //
  // insert(...) andThen getLastError(...)
  //
  // instead

  def insert(docs: Seq[Document])(coll: Collection)(conn: Connected) {
    run(Insert(coll, docs))(conn)
  }

  def insert(doc: Document)(coll: Collection)(conn: Connected) {
    insert(Seq(doc))(coll)(conn)
  }

  def remove(query: Document)(coll: Collection)(conn: Connected) {
    run(Delete(coll, Seq(SingleRemove), query))(conn)
  }
  def removeAll(query: Document)(coll: Collection)(conn: Connected) {
    run(Delete(coll, List.empty, query))(conn)
  }

  def update(query: Document, modifier: Document, opts: Seq[UpdateOption] = List.empty)
            (coll: Collection)(conn: Connected)
  {
    run(Update(coll, opts, query, modifier))(conn)
  }
  def updateAll(query: Document, modifier: Document)(coll: Collection)(conn: Connected) {
    update(query, modifier, Seq(MultiUpdate))(coll)(conn)
  }
  def upsert(query: Document, modifier: Document)(coll: Collection)(conn: Connected) {
    update(query, modifier, Seq(Upsert))(coll)(conn)
  }
  def upsertAll(query: Document, modifier: Document)(coll: Collection)(conn: Connected) {
    update(query, modifier, Seq(Upsert, MultiUpdate))(coll)(conn)
  }


  def findAndUpdate(query: Document      = Document.empty
                  , sort: Document       = Document.empty
                  , modifier: Document   = Document.empty
                  , returnNew: Boolean   = false
                  , projection: Document = Document.empty
      )(coll: Collection)(conn: Connected): Option[Document] = {
    val cmd = (
         ("findandmodify" := coll.name)
      :: ("query" := query)
      :: ("sort" := sort)
      :: ("update" := modifier)
      :: ("new" := returnNew)
      :: ("fields" := projection)
    )
    findAndModify(cmd, coll, conn)
  }

  def findAndUpsert(query: Document      = Document.empty
                  , sort: Document       = Document.empty
                  , modifier: Document   = Document.empty
                  , returnNew: Boolean   = false
                  , projection: Document = Document.empty
      )(coll: Collection)(conn: Connected): Option[Document] = {
    val cmd = (
         ("findandmodify" := coll.name)
      :: ("query" := query)
      :: ("sort" := sort)
      :: ("update" := modifier)
      :: ("new" := returnNew)
      :: ("fields" := projection)
      :: ("upsert" := true)
    )
    findAndModify(cmd, coll, conn)
  }

  def findAndRemove(query: Document      = Document.empty
                  , sort: Document       = Document.empty
                  , projection: Document = Document.empty
      )(coll: Collection)(conn: Connected): Document = {
    val cmd = (
         ("findandmodify" := coll.name)
      :: ("query" := query)
      :: ("sort" := sort)
      :: ("fields" := projection)
      :: ("remove" := true)
    )
    findAndModify(cmd, coll, conn).get
  }

  private def findAndModify(cmd: Document, coll: Collection, conn: Connected): Option[Document] =
   command(cmd)(coll.db)(conn)

  def distinct(key: String, query: Option[Document])
              (coll: Collection)(conn: Connected): Seq[Value] = {
    val cmd = (
         ("distinct" := coll.name)
      :: ("key" := key)
      :: ("query" := (query | Document.empty))
    )

    command(cmd)(coll.db)(conn) map (
      _('values) map {
        case x:ArrayValue => x.vs
      } get
    ) get
  }

// def group
// def mapReduce

  def ensureIndex(keys: Seq[String], unique: Boolean)(coll: Collection)(conn: Connected) {
    insert(("key" := Document(keys map (_ := 1)))
        :: ("unique" := unique)
        :: ("name" := keys.mkString("_"))
        :: ("ns" := toNamespace(coll).toString))(
      Collection("indexes", Database("system"))
    )(conn)
  }

  def dropIndex(keys: Seq[String])(coll: Collection)(conn: Connected) {
    command(("deleteIndexes" := coll.name)
         :: ("index" := keys.mkString("_")))(coll.db)(conn)
  }

  def drop(coll: Collection)(conn: Connected) {
    command("drop" := coll.name)(coll.db)(conn)
  }

  def count(query: Document, skip: Option[Int], limit: Option[Int])
           (coll: Collection)(conn: Connected): Long = {
    val cmd = (
         ("count" := coll.name)
      :: ("query" := query)
      :: ("skip"  := (skip | 0))
      :: ("limit" := (limit | 0))
    )
    command(cmd)(coll.db)(conn) map (
      _('n) map {
        case x:LongValue => x.l
        case _ => -1L
      } get
    ) get
  }

  // database operations
  def drop(db: Database)(conn: Connected) {
    command("dropDatabase" := 1)(db)(conn)
  }

  def getLastError(w: Int = 1, t: Int = 0, fsync: Boolean = false)(db: Database)(conn: Connected) =
    command(("getlasterror" := 1)
         :: ("w" := w)
         :: ("wtimeout" := t)
         :: ("fsync" := fsync))(db)(conn)

  def command(cmd: Document)(db: Database)(conn: Connected): Option[Document] = {
    val q = Query(ns = Collection("$cmd", db)
                , opts = Seq()
                , skip = 0
                , batchSize = 1
                , selector  = cmd
                , projector = None)
    run(q)(conn) map (_.docs.headOption)
  }

// def eval

  // TODO: wrappers for admin commands

  object Implicits {
    implicit def doc2UserQuery(d: Document): UserQuery = UserQuery(Selector(d))
  }
}
