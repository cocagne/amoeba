package com.ibm.amoeba.client

import java.util.UUID
import com.ibm.amoeba.client.tkvl.TieredKeyValueList
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.network.Codec
import com.ibm.amoeba.common.objects.Key
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.codec

object StoragePool {
  private [amoeba] val ConfigKey = Key(Array[Byte](0))
  private [amoeba] val ErrorTreeKey = Key(Array[Byte](1))
  private [amoeba] val AllocationTreeKey = Key(Array[Byte](2))
  private [amoeba] val HostsTreeKey = Key(Array[Byte](3))

  final case class Config(
                         poolId: PoolId,
                         name: String,
                         numberOfStores: Int,
                         defaultIDA: IDA,
                         maxObjectSize: Option[Int],
                         storeHosts: Array[HostId]
                         ):
    def encode(): Array[Byte] = Codec.encode(this).toByteArray

  object Config:
    def apply(cfg: Array[Byte]): Config = Codec.decode(codec.PoolConfig.parseFrom(cfg))

}

trait StoragePool {

  val poolId: PoolId

  val name: String

  val numberOfStores: Int

  val maxObjectSize: Option[Int]

  val defaultIDA: IDA

  val storeHosts: Array[HostId]

  def supportsIDA(ida: IDA): Boolean

  def createAllocater(ida: IDA): ObjectAllocator

  /** Throws AllocationError: UnsupportedIDA if the IDA is not supported*/
  private[client] def selectStoresForAllocation(ida: IDA): Array[Int]

  def allocationTree: TieredKeyValueList

  def errorTree: TieredKeyValueList

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
