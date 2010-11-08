package lol

import java.util.concurrent.atomic.AtomicInteger


object Protocol {

  import Bson._


  type CursorId = Long
  type Namespace = String

  private val reqId = new AtomicInteger(0)
  abstract sealed class Message(val opcode: Int) {
    lazy val requestId = reqId.incrementAndGet
  }
  // Notice is a message which does not expect a reply
  abstract sealed class Notice(oc: Int) extends Message(oc)
  case class Insert(ns: Namespace
                  , docs: Seq[Document]) extends Notice(2002)
  case class Update(ns: Namespace
                  , opts: Seq[UpdateOption]
                  , selector: Document
                  , modifier: Document) extends Notice(2001)
  case class Delete(ns: Namespace
                  , opts: Seq[DeleteOption]
                  , selector: Document) extends Notice(2006)
  case class KillCursors(cursors: Seq[CursorId]) extends Notice(2007)

  // Request is a message with reply
  abstract sealed class Request(oc: Int) extends Message(oc)
  case class Query(ns: Namespace
                 , opts: Seq[QueryOption]
                 , skip: Int
                 , batchSize: Int
                 , selector: Document
                 , projector: Option[Document]) extends Request(2004)
  case class GetMore(ns: Namespace
                   , batchSize: Int
                   , cursorId: CursorId) extends Request(2005)

  case class Header(length: Int
                  , reqId: Int
                  , responseTo: Int
                  , opcode: Int)
  case class Reply(header: Header
                 , flags: Seq[ResponseFlag]
                 , cursorId: CursorId
                 , startingFrom: Int
                 , numReturned: Int
                 , docs: Seq[Document])

  abstract sealed class ResponseFlag(val bit: Int)
  object ResponseFlag {
    def apply(i: Int): Option[ResponseFlag] = i match {
      case 1 => Some(CursorNotFound)
      case 2 => Some(QueryFailure)
      case 4 => Some(ShardConfigStale)
      case 8 => Some(AwaitCapable)
    }

    def unapplySeq(i: Int): Option[Seq[ResponseFlag]] =
      Some(Seq(i & 1, i & 2, i & 4, i & 8) filter (_ > 0) flatMap (apply))
  }
  case object CursorNotFound extends ResponseFlag(1)
  case object QueryFailure extends ResponseFlag(2)
  case object ShardConfigStale extends ResponseFlag(4)
  case object AwaitCapable extends ResponseFlag(8)


  abstract sealed class QueryOption(val bit: Int)
  case object TailableCursor extends QueryOption(2)
  case object SlaveOk extends QueryOption(4)
  case object NoCursorTimeout extends QueryOption(16)
  case object AwaitData extends QueryOption(32)

  abstract sealed class UpdateOption(val bit: Int)
  case object Upsert extends UpdateOption(0)
  case object MultiUpdate extends UpdateOption(1)

  abstract sealed class DeleteOption(val bit: Int)
  case object SingleRemove extends DeleteOption(0)
}
