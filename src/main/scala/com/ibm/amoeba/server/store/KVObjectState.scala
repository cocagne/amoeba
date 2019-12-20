package com.ibm.amoeba.server.store

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{Key, ObjectRevision, Value}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.common.util.Varint

class KVObjectState(
  var min: Option[Key] = None,
  var max: Option[Key] = None,
  var left: Option[Value] = None,
  var right: Option[Value] = None,
  var content: Map[Key, ValueState] = Map(),
  var noExistenceLocks: Map[Key, TransactionId] = Map(),
) {

  def encode(): DataBuffer = {
    KVObjectState.encode(min, max, left, right, content)
  }
}

object KVObjectState {

  def apply(dataBuffer: DataBuffer): KVObjectState = decode(dataBuffer)

  // <mask byte> min 0 max << 1 left << 2 right << 3
  // <vartint len><nbytes>
  // <varint_num_content_entries>
  // <16-byte-revision><8-byte-timestamp><varint_key_len><key><varint_value><value>
  def encodedSize( min: Option[Key],
                   max: Option[Key],
                   left: Option[Value],
                   right: Option[Value],
                   contents: Map[Key, ValueState]): Int = {
    var size = 1 // mask byte
    min.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    max.foreach(key => size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length)
    left.foreach(value => size += Varint.getUnsignedIntEncodingLength(value.bytes.length) + value.bytes.length)
    right.foreach(value => size += Varint.getUnsignedIntEncodingLength(value.bytes.length) + value.bytes.length)
    size += Varint.getUnsignedIntEncodingLength(contents.size)
    contents.foreach { t =>
      val (key, vs) = t
      size += 16 + 8
      size += Varint.getUnsignedIntEncodingLength(key.bytes.length) + key.bytes.length
      size += Varint.getUnsignedIntEncodingLength(vs.value.bytes.length) + vs.value.bytes.length
    }
    size
  }

  def encode( min: Option[Key],
              max: Option[Key],
              left: Option[Value],
              right: Option[Value],
              contents: Map[Key, ValueState]): DataBuffer = {
    val bb = ByteBuffer.allocate(encodedSize(min, max, left, right, contents))

    var mask: Byte = 0
    if (min.nonEmpty)   mask = (mask | 1 << 0).asInstanceOf[Byte]
    if (max.nonEmpty)   mask = (mask | 1 << 1).asInstanceOf[Byte]
    if (left.nonEmpty)  mask = (mask | 1 << 2).asInstanceOf[Byte]
    if (right.nonEmpty) mask = (mask | 1 << 3).asInstanceOf[Byte]
    bb.put(mask)
    min.foreach { key =>
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
    }
    max.foreach { key =>
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
    }
    left.foreach { value =>
      Varint.putUnsignedInt(bb, value.bytes.length)
      bb.put(value.bytes)
    }
    right.foreach { value =>
      Varint.putUnsignedInt(bb, value.bytes.length)
      bb.put(value.bytes)
    }
    Varint.putUnsignedInt(bb, contents.size)
    contents.foreach { t =>
      val (key, vs) = t
      bb.putLong(vs.revision.lastUpdateTxUUID.getMostSignificantBits)
      bb.putLong(vs.revision.lastUpdateTxUUID.getLeastSignificantBits)
      bb.putLong(vs.timestamp.asLong)
      Varint.putUnsignedInt(bb, key.bytes.length)
      bb.put(key.bytes)
      Varint.putUnsignedInt(bb, vs.value.bytes.length)
      bb.put(vs.value.bytes)
    }

    bb.position(0)

    bb
  }

  def decode(db: DataBuffer): KVObjectState = {
    val bb = db.asReadOnlyBuffer()

    def getBytes: Array[Byte] = {
      val len = Varint.getUnsignedInt(bb)
      val arr = new Array[Byte](len)
      bb.get(arr)
      arr
    }

    def getKey: Key = Key(getBytes)
    def getValue: Value = Value(getBytes)

    def getContent: (Key, ValueState) = {
      val msb = bb.getLong
      val lsb = bb.getLong
      val revision = ObjectRevision(TransactionId(new UUID(msb, lsb)))
      val timestamp = HLCTimestamp(bb.getLong)
      val key = getKey
      val value = getValue
      key -> new ValueState(value, revision, timestamp, None)
    }
    val mask = bb.get()

    val min   = if ((mask & 1 << 0) != 0) Some(getKey) else None
    val max   = if ((mask & 1 << 1) != 0) Some(getKey) else None
    val left  = if ((mask & 1 << 2) != 0) Some(getValue) else None
    val right = if ((mask & 1 << 3) != 0) Some(getValue) else None

    val ncontents = Varint.getUnsignedInt(bb)

    var content: Map[Key, ValueState] = Map()

    for (_ <- 0 until ncontents) {
      content += getContent
    }

    new KVObjectState(min, max, left, right, content, Map())
  }
}
