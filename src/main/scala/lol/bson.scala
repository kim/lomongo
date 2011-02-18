package lol

object Bson {

  import org.joda.time.{ DateTime, DateTimeZone }
  import java.util.Date
  import java.util.regex.Pattern
  import collection.mutable.HashMap


  case class Document(fields: Seq[Field]) {
    private val fs: Map[String,Field] =
      fields.foldLeft(new HashMap[String,Field]())(
        (acc,f) => acc += (f.name -> f)
      ).toMap

    def apply(field: String): Option[Field] = fs.get(field)
    def apply(field: Symbol): Option[Field] = fs.get(field.name)

    def ::(f: Field): Document =
      Document((fs + (f.name -> f)).values.toSeq)

    def ++(d: Document): Document =
      Document((fs ++ d.fields.map(f => (f.name -> f))).values.toSeq)

    override def toString: String =
      fields.mkString("[ ", ", ", " ]")
  }
  object Document {
    lazy val empty = Document(Seq())
  }

  case class Field(name: String, value: Value) {
    def ::(f: Field): Document = Document(Seq(this, f))

    override def toString: String =
      "(" + name + " =: " + value + ")"
  }

  abstract sealed class Value(is: Any) {
    def =:(s: String): Field = Field(s, this)

    override def toString: String = Option(is) map (_.toString) getOrElse("null")
  }
  case class DoubleValue(d: Double) extends Value(d)
  case class StringValue(s: String) extends Value(s)
  case class DocumentValue(d: Document) extends Value(d)
  case class ArrayValue(vs: List[Value]) extends Value(vs)
  case class IntValue(i: Int) extends Value(i)
  case class LongValue(l: Long) extends Value(l)
  case class ObjectIdValue(oid: ObjectId) extends Value(oid)
  case class SymbolValue(sym: Symbol) extends Value(sym)
  case class UTCValue(utc: Long) extends Value(utc)
  case class BooleanValue(b: Boolean) extends Value(b)
  case class RegExValue(p: Pattern) extends Value(p)
  case object NullValue extends Value(null)
  case class JavaScriptValue(js: String) extends Value(js)
  //case class CodeWithScopeValue ????

  abstract sealed class BinaryValue(is: Array[Byte]) extends Value(is)
  case class BinaryGenericValue(b: Array[Byte]) extends BinaryValue(b)
  case class FunctionValue(b: Array[Byte]) extends BinaryValue(b)
  case class BinaryOldValue(b: Array[Byte]) extends BinaryValue(b)
  case class UUIDValue(b: Array[Byte]) extends BinaryValue(b)
  case class MD5Value(b: Array[Byte]) extends BinaryValue(b)
  case class UserDefinedValue(b: Array[Byte]) extends BinaryValue(b)

  case class ObjectId(time: Int, machine: Int, inc: Int)
  object ObjectId {

    import java.net.{ NetworkInterface => Iface }
    import java.lang.management.{ ManagementFactory => MX }
    import java.util.concurrent.atomic.AtomicInteger
    import java.util.Random
    import collection.JavaConversions._

    def gen(): ObjectId =
      ObjectId(flip(curt()), machine, flip(inc.getAndIncrement()))

    private lazy val machine = {
      val ifs = Iface.getNetworkInterfaces.foldLeft("")(_ + _).hashCode << 16
      val proc = MX.getRuntimeMXBean().getName().hashCode() & 0xFFFF
      ifs | proc
    }
    private lazy val inc = new AtomicInteger(new Random().nextInt)

    private def curt(): Int = (System.currentTimeMillis/1000).toInt
    private def flip(i: Int): Int = {
      var x: Int = 0
      x |= ((x << 24) & 0xFF000000)
      x |= ((x << 8)  & 0x00FF0000)
      x |= ((x >> 8)  & 0x0000FF00)
      x |= ((x >> 24) & 0x000000FF)
      x
    }
  }


  object Value {
    def apply(v: Double): Option[Value] =
      Option(v) map (DoubleValue(_))

    def apply(v: String): Option[Value] =
      Option(v) map (StringValue(_))

    def apply(v: Document): Option[Value] =
      Option(v) map (DocumentValue(_))

    def apply(v: List[Value]): Option[Value] =
      Option(v) map (ArrayValue(_))

    def apply(v: Int): Option[Value] =
      Option(v) map (IntValue(_))

    def apply(v: Long): Option[Value] =
      Option(v) map (LongValue(_))

    def apply(v: ObjectId): Option[Value] =
      Option(v) map (ObjectIdValue(_))

    def apply(v: Symbol): Option[Value] =
      Option(v) map (SymbolValue(_))

    def apply(v: DateTime): Option[Value] =
      Option(v) map (d => UTCValue(d.getMillis))

    def apply(v: Date): Option[Value] =
      Option(v) map (d => UTCValue(new DateTime(d, DateTimeZone.UTC).getMillis))

    def apply(v: Boolean): Option[Value] =
      Option(v) map (BooleanValue(_))

    def apply(v: Pattern): Option[Value] =
      Option(v) map (RegExValue(_))

    def apply(): Option[Value] = Some(NullValue)
  }

  object Implicits {
    implicit def double2Value(v: Double): Value = DoubleValue(v)
    implicit def string2Value(v: String): Value = StringValue(v)
    implicit def document2Value(v: Document): Value = DocumentValue(v)
    implicit def valueList2Value(v: List[Value]): Value = ArrayValue(v)
    implicit def int2Value(v: Int): Value = IntValue(v)
    implicit def long2Value(v: Long): Value = LongValue(v)
    implicit def objectId2Value(v: ObjectId): Value = ObjectIdValue(v)
    implicit def symbol2Value(v: Symbol): Value = SymbolValue(v)
    implicit def dateTime2Value(v: DateTime): Value = UTCValue(v.getMillis)
    implicit def date2Value(v: Date): Value =
      dateTime2Value(new DateTime(v, DateTimeZone.UTC))
    implicit def boolean2Value(v: Boolean): Value = BooleanValue(v)
    implicit def pattern2Value(v: Pattern): Value = RegExValue(v)

    implicit def field2Document(f: Field): Document = Document(Seq(f))
    implicit def fields2Document(fs: Seq[Field]): Document = Document(fs)
  }
}
