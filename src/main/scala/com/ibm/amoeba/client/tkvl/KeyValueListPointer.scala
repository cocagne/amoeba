package com.ibm.amoeba.client.tkvl

import java.nio.ByteBuffer

import com.ibm.amoeba.client.KeyValueObjectState
import com.ibm.amoeba.common.objects.{Key, KeyValueObjectPointer, Value}
import com.ibm.amoeba.common.util.Varint

final case class KeyValueListPointer(minimum:Key, pointer:KeyValueObjectPointer) {

  def toArray: Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    encodeInto(ByteBuffer.wrap(arr))
    arr
  }

  def encodedSize: Int = Varint.getUnsignedIntEncodingLength(minimum.bytes.length) + minimum.bytes.length + pointer.encodedSize

  def encodeInto(bb: ByteBuffer): Unit = {
    Varint.putUnsignedInt(bb, minimum.bytes.length)
    bb.put(minimum.bytes)
    pointer.encodeInto(bb)
  }
}

object KeyValueListPointer {

  def apply(objectState: KeyValueObjectState): KeyValueListPointer = {
    new KeyValueListPointer(objectState.minimum.getOrElse(Key.AbsoluteMinimum), objectState.pointer)
  }

  def apply(bb:ByteBuffer): KeyValueListPointer = {
    val minLen = Varint.getUnsignedInt(bb)
    val minimum = new Array[Byte](minLen)
    bb.get(minimum)
    val pointer = KeyValueObjectPointer(bb)
    KeyValueListPointer(Key(minimum), pointer)
  }

  def apply(arr: Array[Byte]): KeyValueListPointer = KeyValueListPointer(ByteBuffer.wrap(arr))

}
