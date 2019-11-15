package com.ibm.amoeba.common.objects

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.transaction.{KeyValueUpdate, TransactionId}

object AllocationRevisionGuard {
  def deserialize(db: DataBuffer): AllocationRevisionGuard = {
    val bb = db.asReadOnlyBuffer()
    bb.get() match {
      case 0 =>
        val ptr = ObjectPointer.fromByteBuffer(bb)
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val rev = ObjectRevision(TransactionId(new UUID(msb, lsb)))

        ObjectRevisionGuard(ptr, rev)
      case 1 =>
        val ptr = KeyValueObjectPointer(bb)
        val klen = bb.getInt()
        val karr = new Array[Byte](klen)
        bb.get(karr)
        val key = Key(karr)
        val code = bb.get()
        val ts = bb.getLong()
        val req = code match {
          case 0 => KeyValueUpdate.Exists(key)
          case 1 => KeyValueUpdate.MayExist(key)
          case 2 => KeyValueUpdate.DoesNotExist(key)
          case 3 => KeyValueUpdate.TimestampEquals(key, HLCTimestamp(ts))
          case 4 => KeyValueUpdate.TimestampLessThan(key, HLCTimestamp(ts))
          case 5 => KeyValueUpdate.TimestampGreaterThan(key, HLCTimestamp(ts))
        }

        KeyRevisionGuard(ptr, req)

      case _ => throw new Exception("Unexpected Error")
    }
  }
}

sealed abstract class AllocationRevisionGuard {
  def serialize(): DataBuffer
  val pointer: ObjectPointer
}

case class ObjectRevisionGuard( pointer: ObjectPointer,
                                requiredRevision: ObjectRevision) extends AllocationRevisionGuard {
  def serialize(): DataBuffer = {
    val arr = new Array[Byte](1 + pointer.encodedSize + 16)
    val bb = ByteBuffer.wrap(arr)
    bb.put(0.asInstanceOf[Byte])
    pointer.encodeInto(bb)
    bb.putLong(requiredRevision.lastUpdateTxUUID.getMostSignificantBits)
    bb.putLong(requiredRevision.lastUpdateTxUUID.getLeastSignificantBits)
    arr
  }
}

case class KeyRevisionGuard(
                           pointer: KeyValueObjectPointer,
                           requirement: KeyValueUpdate.KeyRequirement
                           ) extends AllocationRevisionGuard {

  def serialize(): DataBuffer = {
    val arr = new Array[Byte](1 + pointer.encodedSize + 4 + requirement.key.bytes.length + 1 + 8)
    val bb = ByteBuffer.wrap(arr)
    bb.put(1.asInstanceOf[Byte])
    pointer.encodeInto(bb)
    bb.putInt(requirement.key.bytes.length)
    bb.put(requirement.key.bytes)
    val (code, ts) = requirement match {
      case KeyValueUpdate.Exists(_) => (0, 0L)
      case KeyValueUpdate.MayExist(_) => (1, 0L)
      case KeyValueUpdate.DoesNotExist(_) => (2, 0L)
      case KeyValueUpdate.TimestampEquals(_, t) => (3, t.asLong)
      case KeyValueUpdate.TimestampLessThan(_, t) => (4, t.asLong)
      case KeyValueUpdate.TimestampGreaterThan(_, t) => (5, t.asLong)
    }
    bb.put(code.asInstanceOf[Byte])
    bb.putLong(ts)
  }
}
