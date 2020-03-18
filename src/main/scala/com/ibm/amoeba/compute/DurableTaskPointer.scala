package com.ibm.amoeba.compute

import java.nio.ByteBuffer

import com.ibm.amoeba.common.objects.KeyValueObjectPointer

final class DurableTaskPointer(val kvPointer: KeyValueObjectPointer) extends AnyVal {
  def toArray: Array[Byte] = kvPointer.toArray

  def encodedSize: Int = kvPointer.encodedSize

  def encodeInto(bb: ByteBuffer): Unit = kvPointer.encodeInto(bb)
}

object DurableTaskPointer {
  def apply(kvPointer: KeyValueObjectPointer): DurableTaskPointer = new DurableTaskPointer(kvPointer)

  def fromArray(arr: Array[Byte], size: Option[Int]=None): DurableTaskPointer = new DurableTaskPointer(KeyValueObjectPointer(arr))

  def fromByteBuffer(bb: ByteBuffer): DurableTaskPointer = new DurableTaskPointer(KeyValueObjectPointer(bb))

}
