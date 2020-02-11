package com.ibm.amoeba.common.objects

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.transaction.TransactionId

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
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val rev = if (msb == 0 && lsb == 0) None else Some(ObjectRevision(TransactionId(new UUID(msb, lsb))))
        val klen = bb.remaining()
        val karr = new Array[Byte](klen)
        bb.get(karr)

        KeyRevisionGuard(ptr, Key(karr), rev)

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
                           key: Key,
                           keyRevision: Option[ObjectRevision]
                           ) extends AllocationRevisionGuard {

  def serialize(): DataBuffer = {
    val arr = new Array[Byte](1 + pointer.encodedSize + 16 + key.bytes.length)
    val bb = ByteBuffer.wrap(arr)
    bb.put(1.asInstanceOf[Byte])
    pointer.encodeInto(bb)
    keyRevision match {
      case Some(r) => bb.put(r.toArray)
      case None =>
        bb.putLong(0)
        bb.putLong(0)
    }
    bb.put(key.bytes)
  }
}
