package com.ibm.amoeba.client.tkvl

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator}
import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.pool.PoolId

import scala.concurrent.{ExecutionContext, Future}

sealed abstract class NodeAllocator {
  def getAllocatorForTier(tier: Int): Future[ObjectAllocator]

  def getMaxNodeSize(tier: Int): Int

  val code: Int
  def encodedSize: Int
  def encodeInto(bb: ByteBuffer): Unit
}

object NodeAllocator {
  def apply(client: AmoebaClient, bb: ByteBuffer): NodeAllocator = {
    bb.get() match {
      case 0 =>
        val msb = bb.getLong()
        val lsb = bb.getLong()
        val poolId = PoolId(new UUID(msb, lsb))
        new SinglePoolNodeAllocator(client, poolId)
      case _ =>
        println("HERE!!!")
        throw new Exception("Invalid Node Allocator")
    }
  }
}

object BootstrapPoolNodeAllocator extends NodeAllocator {

  val poolId: PoolId = Nucleus.poolId

  override val code: Int = 0
  override val encodedSize = 17

  override def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = null

  override def getMaxNodeSize(tier: Int): Int = 1 * 1024 * 1024

  override def encodeInto(bb:ByteBuffer): Unit = {
    bb.put(code.asInstanceOf[Byte])
    bb.putLong(poolId.uuid.getMostSignificantBits)
    bb.putLong(poolId.uuid.getLeastSignificantBits)
  }
}


class SinglePoolNodeAllocator(val client:AmoebaClient, val poolId: PoolId) extends NodeAllocator {

  implicit val ec: ExecutionContext = client.clientContext

  private var allocator: Option[SinglePoolObjectAllocator] = None

  override val code: Int = 0
  override val encodedSize = 17

  override def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = allocator match {
    case Some(alloc) => Future.successful(alloc)
    case None => client.getStoragePool(poolId).map { pool =>
      val alloc = new SinglePoolObjectAllocator(client, pool, pool.defaultIDA, None)
      // Note, there's a race condition here if multiple getAllocatorForTier calls are made
      // simultaneously. It's harmless though so we'll ignore it and just keep the last one.
      allocator = Some(alloc)
      alloc
    }
  }

  override def getMaxNodeSize(tier: Int): Int = 1 * 1024 * 1024

  override def encodeInto(bb:ByteBuffer): Unit = {
    bb.put(code.asInstanceOf[Byte])
    bb.putLong(poolId.uuid.getMostSignificantBits)
    bb.putLong(poolId.uuid.getLeastSignificantBits)
  }
}