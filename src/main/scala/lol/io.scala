package lol

import scalaz._
import Scalaz._
import org.jboss.{ netty => netty }
import netty.bootstrap.ClientBootstrap
import netty.handler.codec.frame.FrameDecoder
import netty.handler.codec.oneone.OneToOneEncoder
import netty.channel.{ Channel
                     , Channels
                     , ChannelHandlerContext
                     , ChannelFuture
                     , ChannelPipeline
                     , ChannelPipelineFactory
                     , SimpleChannelHandler
                     , ChannelEvent
                     , MessageEvent
                     , ExceptionEvent }
import netty.channel.socket.nio.NioClientSocketChannelFactory
import netty.buffer.{ ChannelBuffer
                    , ChannelBufferIndexFinder => IndexFinder }
import netty.buffer.ChannelBuffers._
import scala.collection.mutable.ListBuffer
import scala.concurrent.SyncVar
import java.util.regex.Pattern
import java.util.concurrent.{ Executors
                            , ConcurrentHashMap }
import java.net.InetSocketAddress


object IO {

  import Protocol._


  case class Connected(channel: Channel, bootstrap: ClientBootstrap)
  case class ReplyPromise(private val svar: SyncVar[Reply]) {
    def get(timeout: Long): Option[Reply] = svar.get(timeout)
    def get: Reply = svar.get
    def map[B](fn: Reply => B): B = fn(get)
  }

  def send(n: Notice)(c: Connected): ChannelFuture =
    c.channel.write(n)

  def send(r: Request)(c: Connected): ReplyPromise = {
    c.channel.write(r)
    ReplyPromise(
      c.channel.getPipeline.getLast.asInstanceOf[ClientHandler]
       .expect(r.requestId))
  }

  def disconnect()(c: Connected) {
    if (c.channel.isConnected)
      c.channel.close.awaitUninterruptibly
    c.bootstrap.releaseExternalResources
  }

  def connect(host: String, port: Int): Option[Connected] = {
    val bootstrap = new ClientBootstrap(
                          new NioClientSocketChannelFactory(
                            Executors.newCachedThreadPool
                          , Executors.newCachedThreadPool))
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      def getPipeline(): ChannelPipeline = {
        Channels.pipeline(new BsonEncoder
                        , new BsonDecoder
                        , new ClientHandler)
      }
    })

    val future = bootstrap.connect(new InetSocketAddress(host, port))
    val channel = future.awaitUninterruptibly.getChannel
    if (!future.isSuccess) {
      future.getCause.printStackTrace
      bootstrap.releaseExternalResources
      None
    }
    else
      Some(Connected(channel, bootstrap))
  }

  class ClientHandler extends SimpleChannelHandler {

    import Protocol._

    private val replies = new ConcurrentHashMap[Int,SyncVar[Reply]]

    def expect(requestId: Int): SyncVar[Reply] =
      Option(
        replies.putIfAbsent(requestId, new SyncVar[Reply])
      ) | replies.get(requestId)

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      e.getMessage match {
        case r:Reply =>
          Option(replies.remove(r.header.responseTo)) map (_ put(r))
      }
      super.messageReceived(ctx, e)
    }
  }

  class BsonEncoder extends OneToOneEncoder {

    import Protocol._
    import Bson._


    def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Object): Object = {
      msg match {
        case x:Message => {
          val buf = dynamicBuffer(LITTLE_ENDIAN, 1024)
          write(Header(0, x.requestId, 0, x.opcode))(buf)
          x match {
            case Insert(ns,docs) => {
              write(0)(buf)
              cstring(ns)(buf)
              docs.map(write(_)(buf))
            }
            case Update(ns,opts,sel,mod) => {
              write(0)(buf)
              cstring(ns)(buf)
              write(opts.map(_.bit).foldLeft(0)((acc,b) => acc | b))(buf)
              write(sel)(buf)
              write(mod)(buf)
            }
            case Delete(ns,opts,sel) => {
              write(0)(buf)
              cstring(ns)(buf)
              write(opts.map(_.bit).foldLeft(0)((acc,b) => acc | b))(buf)
              write(sel)(buf)
            }
            case KillCursors(cs) => {
              write(0)(buf)
              write(cs.length)(buf)
              cs.map(write(_)(buf))
            }
            case Query(ns,opts,skip,batch,sel,proj) => {
              write(opts.map(_.bit).foldLeft(0)((acc,b) => acc | b))(buf)
              cstring(ns)(buf)
              write(skip)(buf)
              write(batch)(buf)
              write(sel)(buf)
              proj map (write(_)(buf))
            }
            case GetMore(ns,batch,cid) => {
              write(0)(buf)
              cstring(ns)(buf)
              write(batch)(buf)
              write(cid)(buf)
            }
          }
          writeAt(0, buf.writerIndex)(buf)
          buf.copy(0, buf.writerIndex)
        }
        case _ => msg
      }
    }

    def write(h: Header)(buf: ChannelBuffer) {
      Seq(h.length, h.reqId, h.responseTo, h.opcode).foreach(write(_)(buf))
    }

    def write(d: Document)(buf: ChannelBuffer) {
      val pos = buf.writerIndex
      write(0)(buf) // size placeholder
      d.fields.reverse.map(write(_)(buf))
      writeAt(pos, buf.writerIndex - pos + 1)(buf)
      buf.writeByte(0)
    }

    def write(f: Field)(buf: ChannelBuffer) {

      def field(magic: Byte, value: ChannelBuffer => Unit) {
        buf.writeByte(magic)
        cstring(f.name)(buf)
        value(buf)
      }

      f.value match {
        case DoubleValue(d)        => field(0x01, write(d))
        case StringValue(s)        => field(0x02, write(s))
        case DocumentValue(d)      => field(0x03, write(d))
        case ArrayValue(vs)        => field(0x04, write(vs))
        case IntValue(i)           => field(0x10, write(i))
        case LongValue(l)          => field(0x12, write(l))
        case ObjectIdValue(oid: ObjectId) => field(0x07, write(oid))
        case SymbolValue(sym: Symbol)     => field(0x0E, write(sym.name))
        case UTCValue(utc)         => field(0x09, write(utc))
        case BooleanValue(b)       => field(0x08, write(b))
        case RegExValue(p)         => field(0x0B, write(p))
        case NullValue             => field(0x0A, (_ => ()))
        case JavaScriptValue(js)   => field(0x0D, write(js))
        case BinaryGenericValue(b) => field(0x05, binary(b, 0))
        case FunctionValue(b)      => field(0x05, binary(b, 1))
        case BinaryOldValue(b)     => field(0x05, binary(b, 2))
        case UUIDValue(b)          => field(0x05, binary(b, 3))
        case MD5Value(b)           => field(0x05, binary(b, 5))
        case UserDefinedValue(b)   => field(0x05, binary(b, 128))
      }
    }

    def write(d: Double)(buf: ChannelBuffer) {
      write(java.lang.Double.doubleToRawLongBits(d))(buf)
    }

    def write(s: String)(buf: ChannelBuffer) {
      val pos = buf.writerIndex
      write(0)(buf)
      cstring(s)(buf)
      writeAt(pos, s.length + 1)(buf)
    }

    def write(i: Int)(buf: ChannelBuffer) {
      for(x <- Seq(0,8,16,24))
        buf.writeByte((0xFF & (i >> x)).asInstanceOf[Byte])
    }

    def write(l: Long)(buf: ChannelBuffer) {
      for(x <- Seq(0,8,16,24,32,40,48,56))
        buf.writeByte((0xFFL & (l >> x)).asInstanceOf[Byte])
    }

    def writeAt(index: Int, i: Int)(buf: ChannelBuffer) {
      val pos = buf.writerIndex
      buf.writerIndex(index)
      write(i)(buf)
      buf.writerIndex(pos)
    }

    def writeAt(index: Int, l: Long)(buf: ChannelBuffer) {
      val pos = buf.writerIndex
      buf.writerIndex(index)
      write(l)(buf)
      buf.writerIndex(pos)
    }

    def write(b: Boolean)(buf: ChannelBuffer) {
      if (b) buf.writeByte(0x01) else buf.writeByte(0)
    }

    def write(p: Pattern)(buf: ChannelBuffer) {
      val (regex,options) = bsonRegex(p)
      cstring(regex)(buf)
      cstring(options)(buf)
    }

    def write(xs: List[Value])(buf: ChannelBuffer) {
      import Bson.Implicits._

      val d = xs.foldLeft(Document.empty)((doc,v) => {
        (doc.fields.length.toString := v) :: doc
      })
      write(d)(buf)
    }

    def write(oid: ObjectId)(buf: ChannelBuffer) {
      write(oid.time)(buf)
      write(oid.machine)(buf)
      write(oid.inc)(buf)
    }

    def cstring(s: String)(buf: ChannelBuffer) {
      var i = 0
      while(i < s.length) {
        val c = Character.codePointAt(s, i);
        if (c < 0x80)
          buf.writeByte(c)
        else if (c < 0x800) {
          buf.writeByte((0xc0 + (c >> 6)))
          buf.writeByte((0x80 + (c & 0x3f)))
        }
        else if (c < 0x10000) {
          buf.writeByte((0xe0 + (c >> 12)))
          buf.writeByte((0x80 + ((c >> 6) & 0x3f)))
          buf.writeByte((0x80 + (c & 0x3f)))
        }
        else {
          buf.writeByte((0xf0 + (c >> 18)))
          buf.writeByte((0x80 + ((c >> 12) & 0x3f)))
          buf.writeByte((0x80 + ((c >> 6) & 0x3f)))
          buf.writeByte((0x80 + (c & 0x3f)))
        }

        i += Character.charCount(c)
      }
      buf.writeByte(0)
    }

    def binary(bs: Array[Byte], subtype: Int)(buf: ChannelBuffer) {
      write(bs.length)(buf)
      write(subtype)(buf)
      buf.writeBytes(bs)
    }

    private val flags = Map(
        256 -> 'g'
      , Pattern.CASE_INSENSITIVE -> 'i'
      , Pattern.MULTILINE -> 'm'
      , Pattern.COMMENTS -> 'x'
    )
    def bsonRegex(p: Pattern): (String,String) = {
      (p.pattern
      , flags.keys.foldLeft("")((s,f) =>
          if ((p.flags & f) > 0) s + flags(f) else s
      ))
    }
  }


  class BsonDecoder extends FrameDecoder {

    import Protocol._
    import Bson._


    override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer): Object = {
      if (buf.readableBytes < 4) {
        //println("not enough data")
        return null
      }

      buf.markReaderIndex
      val length = readInt(buf) - 4
      buf.resetReaderIndex

      if (buf.readableBytes < length) {
        //println("frame incomplete (" + length + ":" + buf.readableBytes + ")")
        return null
      }

      val frame = buf.readBytes(length + 4)
      readReply(frame)
    }

    def readReply(frame: ChannelBuffer): Reply = {
      val h = readHeader(frame)
      val ResponseFlag(flags@_*) = readInt(frame)
      val cid   = readLong(frame)
      val start = readInt(frame)
      val num   = readInt(frame)
      val docs  = for(_ <- 1 to num) yield readDocument(frame)
      Reply(h, flags, cid, start, num, docs.toList)
    }

    def readHeader(frame: ChannelBuffer): Header = {
      val Seq(l,req,res,op) = for(_ <- 0 to 3) yield readInt(frame)
      Header(l, req, res, op)
    }

    def readDocument(slice: ChannelBuffer): Document = {
      readInt(slice) // discard size
      val fs = ListBuffer[Field]()
      def readFields {
        readField(slice) >>= (f => {
          fs += f
          if (slice.readable) {
            slice.markReaderIndex
            val term = slice.readByte
            slice.resetReaderIndex
            if (term != 0)
              readFields
            else
              slice.skipBytes(1)
          }
          none
        })
      }
      readFields
      Document(fs.toList)
    }

    def readField(slice: ChannelBuffer): Option[Field] = {
      val magic = slice.readByte
      if (magic == 0) return None

      val name  = readCString(slice)
      val value = magic match {
        case 0x01  => Some(DoubleValue(java.lang.Double.longBitsToDouble(readLong(slice))))
        case 0x02  => Some(StringValue(readString(slice)))
        case 0x03  => Some(DocumentValue(readDocument(slice))) // TODO: slice?
        case 0x04  => Some(ArrayValue(readDocument(slice).fields.map(_.value).toList))
        case 0x10  => Some(IntValue(readInt(slice)))
        case 0x12  => Some(LongValue(readLong(slice)))
        case 0x07  => {
          val time = readInt(slice)
          val mach = readInt(slice)
          val incr = readInt(slice)
          Some(ObjectIdValue(ObjectId(time,mach,incr)))
        }
        case 0x0E  => Some(SymbolValue(Symbol(readString(slice))))
        case 0x09  => Some(UTCValue(readLong(slice)))
        case 0x08  => Some(BooleanValue(slice.readByte == 1))
        case 0x0B  => {
          val pattern = readCString(slice)
          val options = readCString(slice).map({
            case 'i' => Pattern.CASE_INSENSITIVE
            case 'g' => 256
            case 'm' => Pattern.MULTILINE
            case 'x' => Pattern.COMMENTS
            case _ => 0
          }).foldLeft(0)((acc,opt) => acc | opt)
          Some(RegExValue(Pattern.compile(pattern, options)))
        }
        case 0x0A  => Some(NullValue)
        case 0x0D  => Some(JavaScriptValue(readString(slice)))
        case 0x05  => Some(readBinaryValue(slice))
        case _ => {
          //println("unknown magic: " + magic)
          None
        }
      }

      value map (Field(name, _))
    }

    def readString(buf: ChannelBuffer): String = {
      val length = readInt(buf) - 1
      val s = new String(buf.readBytes(length).toByteBuffer.array, "UTF-8")
      buf.skipBytes(1)
      s
    }

    def readCString(buf: ChannelBuffer): String = {
      val cstring = new String(buf.readBytes(buf.bytesBefore(IndexFinder.NUL))
                                  .toByteBuffer.array, "UTF-8")
      buf.skipBytes(1)
      cstring
    }

    def readBinaryValue(buf: ChannelBuffer): BinaryValue = {
      val length = readInt(buf)
      val subtype = buf.readByte
      def read: Array[Byte] =
        buf.readBytes(length).toByteBuffer.array

      subtype match {
        case 1 => FunctionValue(read)
        case 2 => BinaryOldValue(read)
        case 3 => UUIDValue(read)
        case 5 => MD5Value(read)
        case 128 => UserDefinedValue(read)
        case _ => BinaryGenericValue(read)
      }
    }

    def readInt(buf: ChannelBuffer): Int =
      (0 to 24 by 8)
        .zip(buf.readBytes(4).array)
        .foldLeft(0)((acc,x) => acc | ((0xFF & x._2) << x._1))

    def readLong(buf: ChannelBuffer): Long =
      (0 to 56 by 8)
        .zip(buf.readBytes(8).array)
        .foldLeft(0)((acc,x) => acc | ((0xFF & x._2) << x._1))
  }
}

