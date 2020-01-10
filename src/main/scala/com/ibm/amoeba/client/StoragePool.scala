package com.ibm.amoeba.client

import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.pool.PoolId

trait StoragePool {

  val poolId: PoolId

  val numberOfStores: Int

  val maxObjectSize: Option[Int]

  def supportsIDA(ida: IDA): Boolean

  def createAllocater(ida: IDA): ObjectAllocator

  /** Throws AllocationError: UnsupportedIDA if the IDA is not supported*/
  private[client] def selectStoresForAllocation(ida: IDA): Array[Int]

  /*
  val poolDefinitionPointer: KeyValueObjectPointer

  def getAllocationTree(retryStrategy: RetryStrategy)(implicit ec: ExecutionContext): Future[MutableTieredKeyValueList]

  def getAllocatedObjectsIterator()(implicit ec: ExecutionContext): Future[AllocatedObjectsIterator]

  /** The entries of this array describe which storage host that currently owns store with the corresponding index */
  def storageHosts: Array[StorageHost]

  def numberOfStores: Int = storageHosts.length

  def supportsIDA(ida: IDA): Boolean

  /** Throws AllocationError: UnsupportedIDA if the IDA is not supported*/
  def selectStoresForAllocation(ida: IDA): Array[Int]

  def getMissedUpdateStrategy(): MissedUpdateStrategy

  def createMissedUpdateHandler(
                                 transactionUUID: UUID,
                                 pointer: ObjectPointer,
                                 missedStores: List[Byte])(implicit ec: ExecutionContext): MissedUpdateHandler

  def createMissedUpdateIterator(poolIndex: Byte)(implicit ec: ExecutionContext): MissedUpdateIterator

  def refresh()(implicit ec: ExecutionContext): Future[StoragePool]

   */
}
