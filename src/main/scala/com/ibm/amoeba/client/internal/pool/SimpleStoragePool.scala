package com.ibm.amoeba.client.internal.pool

import java.util.UUID

import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator, StoragePool}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.pool.PoolId

class SimpleStoragePool(val client: AmoebaClient,
                        val poolId: PoolId,
                        val numberOfStores: Int,
                        val defaultIDA: IDA,
                        val allocationTreeAllocatorConfig: Option[UUID],
                        val maxObjectSize: Option[Int] = None) extends StoragePool {

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
