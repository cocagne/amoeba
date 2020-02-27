package com.ibm.amoeba.client.internal.pool

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import com.ibm.amoeba.client.{AmoebaClient, KeyValueObjectState, ObjectAllocator, StoragePool}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.pool.PoolId

object SimpleStoragePool {

  def encode(poolId: PoolId,
             numberOfStores: Int,
             defaultIDA: IDA,
             allocationTreeAllocatorConfig: Option[UUID],
             maxObjectSize: Option[Int]): Array[Byte] = {
    val arr = new Array[Byte](16 + 1 + IDA.EncodedIDASize + 16 + 4)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(poolId.uuid.getMostSignificantBits)
    bb.putLong(poolId.uuid.getLeastSignificantBits)
    bb.put(numberOfStores.asInstanceOf[Byte])
    defaultIDA.serializeIDAType(bb)
    val ac = allocationTreeAllocatorConfig.getOrElse(new UUID(0, 0))
    bb.putLong(ac.getMostSignificantBits)
    bb.putLong(ac.getLeastSignificantBits)
    bb.putInt(maxObjectSize.getOrElse(0))
    arr
  }

  def apply(client: AmoebaClient,
            kvos: KeyValueObjectState): SimpleStoragePool = {

    val bb = ByteBuffer.wrap(kvos.contents(StoragePool.ConfigKey).value.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    val pmsb = bb.getLong()
    val plsb = bb.getLong()
    val poolId = PoolId(new UUID(pmsb, plsb))
    val numberOfStores = bb.get()
    val defaultIDA = IDA.deserializeIDAType(bb)
    val amsb = bb.getLong()
    val alsb = bb.getLong()
    val allocationTreeAllocatorConfig = if (amsb == 0 && alsb == 0) None else Some(new UUID(amsb, alsb))
    val osize = bb.getInt()
    val maxObjectSize = if (osize == 0) None else Some(osize)

    val allocTree = new TieredKeyValueList(client,
      new KVObjectRootManager(client, StoragePool.AllocationTreeKey, kvos.pointer))

    val errorTree = new TieredKeyValueList(client,
      new KVObjectRootManager(client, StoragePool.ErrorTreeKey, kvos.pointer))

    new SimpleStoragePool(client, poolId, numberOfStores, defaultIDA, allocationTreeAllocatorConfig, maxObjectSize,
      allocTree, errorTree)
  }
}

class SimpleStoragePool(val client: AmoebaClient,
                        val poolId: PoolId,
                        val numberOfStores: Int,
                        val defaultIDA: IDA,
                        val allocationTreeAllocatorConfig: Option[UUID],
                        val maxObjectSize: Option[Int],
                        val allocationTree: TieredKeyValueList,
                        val errorTree: TieredKeyValueList) extends StoragePool {

  override def supportsIDA(ida: IDA): Boolean = numberOfStores >= ida.width

  override def createAllocater(ida: IDA): ObjectAllocator = new SinglePoolObjectAllocator(client,
    this, ida, maxObjectSize)

  override def selectStoresForAllocation(ida: IDA): Array[Int] = {
    val arr = new Array[Int](ida.width)
    for (i <- 0 until ida.width)
      arr(i) = i
    arr
  }
}
