package com.ibm.amoeba.common.objects

import java.nio.ByteBuffer

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.ida.{IDA, Replication}
import com.ibm.amoeba.common.util.Varint

sealed abstract class KeyValueOperation {

  import KeyValueOperation._

  def timestamp: Option[HLCTimestamp]

  def revision: Option[ObjectRevision]

  val opcode: Byte

  final def encodedLength(ida: IDA): Int = {
    val dataLen = dataLength(ida)
    1 + revision.map(_ => 16).getOrElse(0) + timestamp.map(_ => 8).getOrElse(0) + Varint.getUnsignedIntEncodingLength(dataLen) + dataLen
  }

  final def encodedLength: Int = {
    val dataLen = dataLength
    1 + revision.map(_ => 16).getOrElse(0) + timestamp.map(_ => 8).getOrElse(0) + Varint.getUnsignedIntEncodingLength(dataLen) + dataLen
  }

  protected def dataLength(ida: IDA): Int
  protected def putData(ida: IDA, bbArray: Array[ByteBuffer]): Unit

  protected def dataLength: Int
  protected def putData(bb: ByteBuffer): Unit

  final protected def encodeGenericIda(ida: IDA, bbArray:Array[ByteBuffer]): Unit = {

    val mask = revision.map(_ => HasRevisionMask).getOrElse(0.asInstanceOf[Byte]) | timestamp.map(_ => HasTimestampMask).getOrElse(0.asInstanceOf[Byte])
    val dataLen = dataLength(ida)

    for (bb <- bbArray) {
      bb.put( (mask | opcode).asInstanceOf[Byte] )
      revision.foreach(rev => rev.encodeInto(bb))
      timestamp.foreach(ts => bb.putLong(ts.asLong))
      Varint.putUnsignedInt(bb, dataLen)
    }

    putData(ida, bbArray)
  }

  final protected def encodeReplicated(bb: ByteBuffer): Unit = {
    val mask = revision.map(_ => HasRevisionMask).getOrElse(0.asInstanceOf[Byte]) | timestamp.map(_ => HasTimestampMask).getOrElse(0.asInstanceOf[Byte])
    val dataLen = dataLength

    bb.put( (mask | opcode).asInstanceOf[Byte] )
    revision.foreach(rev => rev.encodeInto(bb))
    timestamp.foreach(ts => bb.putLong(ts.asLong))
    Varint.putUnsignedInt(bb, dataLen)
    putData(bb)
  }
}

/**
  * Encoding Format:
  *    Sequence of: <code>[16-byte object-revision][8-byte timestamp]<varint-data-len><data>
  *
  *    <code> is a bitmask with the highest bit being "Has Revision" and second highest being "Has Timestamp". The
  *           remainder is the kind of encoded entry.
  */
object KeyValueOperation {
  val HasRevisionMask: Byte  = (1 << 7).asInstanceOf[Byte]
  val HasTimestampMask: Byte = (1 << 6).asInstanceOf[Byte]
  val CodeMask: Byte         = (0xFF & ~(HasRevisionMask | HasTimestampMask)).asInstanceOf[Byte]

  val SetMinCode: Byte      = 0.asInstanceOf[Byte] // Replicated
  val SetMaxCode: Byte      = 1.asInstanceOf[Byte] // Replicated
  val SetLeftCode: Byte     = 2.asInstanceOf[Byte] // IDA-encoded
  val SetRightCode: Byte    = 3.asInstanceOf[Byte] // IDA-encoded
  val InsertCode: Byte      = 4.asInstanceOf[Byte] // IDA-encoded value
  val DeleteCode: Byte      = 5.asInstanceOf[Byte] // Not stored
  val DeleteMinCode: Byte   = 6.asInstanceOf[Byte] // Not stored
  val DeleteMaxCode: Byte   = 7.asInstanceOf[Byte] // Not stored
  val DeleteLeftCode: Byte  = 8.asInstanceOf[Byte] // Not stored
  val DeleteRightCode: Byte = 9.asInstanceOf[Byte] // Not stored

  def getArray(bb: ByteBuffer, nbytes: Int): Array[Byte] = {
    val arr = new Array[Byte](nbytes)
    bb.get(arr)
    arr
  }

  /** Reads and returns the list of KeyValueOperations contained within the provided ByteBuffer
    *
    *  The position of the buffer is advanced to the end of the operation
    */
  def decode(bb:ByteBuffer, txRevision: ObjectRevision, txTimestamp: HLCTimestamp): List[KeyValueOperation] = {
    var rlist: List[KeyValueOperation] = Nil

    while (bb.remaining() != 0) {
      val mask = bb.get()

      val rev = if ((mask & HasRevisionMask) == 0) txRevision else ObjectRevision(bb)
      val ts = if ((mask & HasTimestampMask) == 0) txTimestamp else HLCTimestamp(bb.getLong())
      val code = mask & CodeMask
      val dataLen = Varint.getUnsignedInt(bb)

      val op = code match {
        case SetMinCode      => SetMin.decode(bb, dataLen, rev, ts)
        case SetMaxCode      => SetMax.decode(bb, dataLen, rev, ts)
        case SetLeftCode     => SetLeft.decode(bb, dataLen, rev, ts)
        case SetRightCode    => SetRight.decode(bb, dataLen, rev, ts)
        case InsertCode      => Insert.decode(bb, dataLen, rev, ts)
        case DeleteCode      => Delete.decode(bb, dataLen)
        case DeleteMinCode   => DeleteMin.decode(bb, dataLen)
        case DeleteMaxCode   => DeleteMax.decode(bb, dataLen)
        case DeleteLeftCode  => DeleteLeft.decode(bb, dataLen)
        case DeleteRightCode => DeleteRight.decode(bb, dataLen)

        case unknownOpCode => throw new KeyValueEncodingError(s"Unknown KeyValue opcode $unknownOpCode")
      }

      rlist = op :: rlist
    }

    rlist.reverse
  }

  def encode(ops: List[KeyValueOperation], ida: IDA): Array[DataBuffer] = {
    val result = new Array[DataBuffer](ida.width)

    val sz = ops.foldLeft(0)( (sz, op) => sz + op.encodedLength(ida))

    ida match {
      case _: Replication =>
        val arr = new Array[Byte](sz)
        val db = DataBuffer(arr)
        val bb = ByteBuffer.wrap(arr)
        for (i <- result.indices)
          result(i) = db
        ops.foreach(op => op.encodeReplicated(bb))

      case _ =>
        val bbs = new Array[ByteBuffer](result.length)
        for (i <-result.indices) {
          val arr = new Array[Byte](sz)
          result(i) = DataBuffer(arr)
          bbs(i) = ByteBuffer.wrap(arr)
        }
        ops.foreach(op => op.encodeGenericIda(ida, bbs))
    }

    result
  }
}

sealed abstract class NoValue extends KeyValueOperation {

  protected def dataLength(ida: IDA): Int = 0
  protected def putData(ida: IDA, bbArray: Array[ByteBuffer]): Unit = {}

  protected def dataLength: Int = 0
  protected def putData(bb: ByteBuffer): Unit = {}

}

sealed abstract class SingleReplicatedValue(val value: Array[Byte]) extends KeyValueOperation {

  protected def dataLength(ida: IDA): Int = value.length
  protected def putData(ida: IDA, bbArray: Array[ByteBuffer]): Unit = bbArray.foreach(bb => bb.put(value))

  protected def dataLength: Int = value.length
  protected def putData(bb: ByteBuffer): Unit = bb.put(value)

}

sealed abstract class SingleEncodedValue(val value: Array[Byte]) extends KeyValueOperation {

  protected def dataLength(ida: IDA): Int = ida.calculateEncodedSegmentLength(value.length)
  protected def putData(ida: IDA, bbArray: Array[ByteBuffer]): Unit = {
    ida.encode(value).zip(bbArray).foreach( t => t._2.put(t._1) )
  }

  protected def dataLength: Int = value.length
  protected def putData(bb: ByteBuffer): Unit = bb.put(value)

}

class SetMin(value: Key, val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleReplicatedValue(value.bytes) {
  val opcode: Byte = KeyValueOperation.SetMinCode
}
object SetMin {
  def apply(value: Key, timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetMin(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp): KeyValueOperation = {
    new SetMin(Key(KeyValueOperation.getArray(bb, dataLen)), Some(timestamp), Some(revision))
  }
}

class SetMax(value: Key, val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleReplicatedValue(value.bytes) {
  val opcode: Byte = KeyValueOperation.SetMaxCode
}
object SetMax {
  def apply(value: Key, timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetMax(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp): SetMax = {
    new SetMax(Key(KeyValueOperation.getArray(bb, dataLen)), Some(timestamp), Some(revision))
  }
}

class SetLeft(value: Array[Byte], val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleEncodedValue(value) {
  val opcode: Byte = KeyValueOperation.SetLeftCode
}
object SetLeft {
  def apply(value: Array[Byte], timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetLeft(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp): SetLeft = {
    new SetLeft(KeyValueOperation.getArray(bb, dataLen), Some(timestamp), Some(revision))
  }
}

class SetRight(value: Array[Byte], val timestamp: Option[HLCTimestamp]=None, val revision: Option[ObjectRevision]=None)  extends SingleEncodedValue(value) {
  val opcode: Byte = KeyValueOperation.SetRightCode
}
object SetRight {
  def apply(value: Array[Byte], timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new SetRight(value, timestamp, revision)
  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp): SetRight = {
    new SetRight(KeyValueOperation.getArray(bb, dataLen), Some(timestamp), Some(revision))
  }
}

class Delete(val key: Key)  extends SingleReplicatedValue(key.bytes) {
  val opcode: Byte = KeyValueOperation.DeleteCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object Delete {
  def apply(key: Key): Delete = new Delete(key)
  def apply(value: Array[Byte]) = new Delete(Key(value))
  def decode(bb: ByteBuffer, dataLen: Int): Delete = {
    new Delete(Key(KeyValueOperation.getArray(bb, dataLen)))
  }
}

class Insert(
              val key: Key,
              val value: Array[Byte],
              val timestamp: Option[HLCTimestamp] = None,
              val revision: Option[ObjectRevision] = None) extends KeyValueOperation {

  val opcode: Byte = KeyValueOperation.InsertCode

  protected def dataLength(ida: IDA): Int = Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length + ida.calculateEncodedSegmentLength(value.length)
  protected def putData(ida: IDA, bbArray: Array[ByteBuffer]): Unit = {
    ida.encode(value).zip(bbArray).foreach { t =>
      val (idaEncodedValue, bb) = t
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
      bb.put(idaEncodedValue)
    }
  }

  protected def dataLength: Int = Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length + value.length
  protected def putData(bb: ByteBuffer): Unit = {
    Varint.putUnsignedInt(bb, key.bytes.length)
    bb.put(key.bytes)
    bb.put(value)
  }

}
object Insert {
  def apply(key: Key, value: Array[Byte], timestamp: Option[HLCTimestamp]=None, revision: Option[ObjectRevision]=None) = new Insert(key, value, timestamp, revision)

  def decode(bb: ByteBuffer, dataLen: Int, revision: ObjectRevision, timestamp: HLCTimestamp): KeyValueOperation = {
    val keyLen = Varint.getUnsignedInt(bb)
    val valLen = dataLen - Varint.getUnsignedIntEncodingLength(keyLen) - keyLen
    val key = Key(KeyValueOperation.getArray(bb, keyLen))
    val value = KeyValueOperation.getArray(bb, valLen)
    new Insert(key, value, Some(timestamp), Some(revision))
  }
}

class DeleteMin extends NoValue {
  val opcode: Byte = KeyValueOperation.DeleteMinCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteMin {
  def apply(): DeleteMin = new DeleteMin()
  def decode(bb: ByteBuffer, dataLen: Int): DeleteMin = {
    new DeleteMin()
  }
}

class DeleteMax extends NoValue {
  val opcode: Byte = KeyValueOperation.DeleteMaxCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteMax {
  def apply(): DeleteMax = new DeleteMax()
  def decode(bb: ByteBuffer, dataLen: Int): DeleteMax = {
    new DeleteMax()
  }
}

class DeleteRight extends NoValue {
  val opcode: Byte = KeyValueOperation.DeleteRightCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteRight {
  def apply(): DeleteRight = new DeleteRight()
  def decode(bb: ByteBuffer, dataLen: Int): DeleteRight = {
    new DeleteRight()
  }
}

class DeleteLeft extends NoValue {
  val opcode: Byte = KeyValueOperation.DeleteLeftCode
  def timestamp: Option[HLCTimestamp] = None
  def revision: Option[ObjectRevision] = None
}
object DeleteLeft {
  def apply(): DeleteLeft = new DeleteLeft()
  def decode(bb: ByteBuffer, dataLen: Int): DeleteLeft = {
    new DeleteLeft()
  }
}
