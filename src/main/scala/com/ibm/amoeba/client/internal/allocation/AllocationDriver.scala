package com.ibm.amoeba.client.internal.allocation

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, ObjectId, ObjectPointer, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}

import scala.concurrent.Future

trait AllocationDriver {


  def futureResult: Future[ObjectPointer]

  /** Immediately cancels all future activity scheduled for execution */
  def shutdown(): Unit

  /** Initiates the allocation process */
  def start(): Unit

  def receiveAllocationResult(fromStoreId: StoreId,
                              result: Option[StorePointer]): Unit
}

object AllocationDriver {
  trait Factory {
    def create(client: AmoebaClient,
               poolId: PoolId,
               newObjectId: ObjectId,
               objectSize: Option[Int],
               objectIDA: IDA,
               objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
               objectType: ObjectType.Value,
               timestamp: HLCTimestamp,
               initialRefcount: ObjectRefcount,
               allocationTransactionId: TransactionId,
               revisionGuard: AllocationRevisionGuard): AllocationDriver
  }

}
