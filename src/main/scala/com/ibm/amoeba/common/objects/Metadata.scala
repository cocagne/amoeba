package com.ibm.amoeba.common.objects

import java.nio.ByteBuffer
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.transaction.TransactionId

import java.util.UUID

case class Metadata(revision: ObjectRevision,
                    refcount: ObjectRefcount,
                    timestamp: HLCTimestamp){

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](Metadata.EncodedSize)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }

  def encodeInto(bb: ByteBuffer): ByteBuffer = {
    revision.encodeInto(bb)
    refcount.encodeInto(bb)
    bb.putLong(timestamp.asLong)
  }
}

object Metadata {
  val EncodedSize: Int = ObjectRevision.EncodedSize + ObjectRefcount.EncodedSize + HLCTimestamp.EncodedSize

  val Zeroed: Metadata = Metadata(
    ObjectRevision(TransactionId(new UUID(0,0))),
    ObjectRefcount(0,0),
    HLCTimestamp(0))

  def apply(arr: Array[Byte]): Metadata = Metadata(ByteBuffer.wrap(arr))

  def apply(bb: ByteBuffer): Metadata = {
    val revision = ObjectRevision(bb)
    val refcount = ObjectRefcount(bb)
    val timestamp = HLCTimestamp(bb.getLong())
    new Metadata(revision, refcount, timestamp)
  }
}
