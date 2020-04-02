package com.ibm.amoeba.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}

import com.ibm.amoeba.client.{AmoebaClient, Transaction}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, Key, KeyOrdering, KeyValueObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future}

case class Root(tier: Int,
                ordering: KeyOrdering,
                rootObject: KeyValueObjectPointer,
                nodeAllocator: NodeAllocator) {

  def encodedSize: Int = 1 + 1 + rootObject.encodedSize + nodeAllocator.encodedSize

  def encode(): Array[Byte] = {
    val arr = new Array[Byte](encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(tier.asInstanceOf[Byte])
    bb.put(ordering.code)
    rootObject.encodeInto(bb)
    nodeAllocator.encodeInto(bb)
    arr
  }

  def encodeInto(bb: ByteBuffer): Unit = {
    bb.put(this.encode())
  }
}

object Root {

  def create(client: AmoebaClient,
             guard: AllocationRevisionGuard,
             ordering: KeyOrdering,
             nodeAllocator: NodeAllocator,
             initialContent: Map[Key, Value] = Map())(implicit t: Transaction): Future[Root] = {

    implicit val ec: ExecutionContext = client.clientContext

    for {
      alloc <- nodeAllocator.getAllocatorForTier(0)
      rp <- alloc.allocateKeyValueObject(guard, initialContent)
    } yield {
      new Root(0, ordering, rp, nodeAllocator)
    }
  }

  def apply(client: AmoebaClient, arr: Array[Byte]): Root = {
    val bb = ByteBuffer.wrap(arr)
    Root(client, bb)
  }

  def apply(client: AmoebaClient, bb: ByteBuffer): Root = {
    bb.order(ByteOrder.BIG_ENDIAN)
    val tier = bb.get()
    val ordering = KeyOrdering.fromCode(bb.get())
    val rootObject = KeyValueObjectPointer(bb)
    val nodeAllocator = NodeAllocator(client, bb)

    Root(tier, ordering, rootObject, nodeAllocator)
  }
}
