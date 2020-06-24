package com.ibm.amoeba.client.internal.allocation

import com.ibm.amoeba.client.{AllocationError, AmoebaClient}
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.network.Allocate
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{Future, Promise}

class BaseAllocationDriver (
                             val client: AmoebaClient,
                             val poolId: PoolId,
                             val newObjectId: ObjectId,
                             val objectSize: Option[Int],
                             val objectIDA: IDA,
                             val objectData: Map[Byte,DataBuffer], // Map DataStore pool index -> store-specific ObjectData
                             val objectType: ObjectType.Value,
                             val timestamp: HLCTimestamp,
                             val initialRefcount: ObjectRefcount,
                             val allocationTransactionId: TransactionId,
                             val revisionGuard: AllocationRevisionGuard
                           ) extends AllocationDriver with Logging {

  private[this] val promise = Promise[ObjectPointer]

  def futureResult: Future[ObjectPointer] = promise.future

  private[this] var responses =  Map[Byte, Option[StorePointer]]()

  def shutdown(): Unit = {}

  /** Initiates the allocation process */
  def start(): Unit = sendAllocationMessages()

  protected def sendAllocationMessages(): Unit = {
    val toSend = synchronized { objectData.filter( t => !responses.contains(t._1) ) }

    for ( (storeIndex, objectData) <- toSend ) {
      val storeId = StoreId(poolId, storeIndex)

      val msg = Allocate(storeId, client.clientId, newObjectId, objectType, objectSize, initialRefcount, objectData, timestamp,
        allocationTransactionId, revisionGuard)

      client.messenger.sendClientRequest(msg)
    }
  }

  def receiveAllocationResult(fromStoreId: StoreId,
                              result: Option[StorePointer]): Unit = synchronized {
    if (promise.isCompleted)
      return // Already done, nothing left to do

    logger.trace(s"Got Allocation Result from store $fromStoreId: $result. Num Responses: ${responses.size}")

    if ( !responses.contains(fromStoreId.poolIndex) )
      responses += (fromStoreId.poolIndex -> result)

    if (responses.size == objectData.size) {
      var errors = Set[Byte]()
      var pointers = List[StorePointer]()

      responses.foreach(t => t._2 match {
        case Some(ptr) => pointers = ptr :: pointers
        case None => errors += t._1
      })
      logger.trace(s"   Errors: $errors")
      if (errors.isEmpty) {
        val sortedPointersArray = pointers.sortBy(sp => sp.poolIndex).toArray
        val op = objectType match {
          case ObjectType.Data => new DataObjectPointer(newObjectId, poolId, objectSize, objectIDA, sortedPointersArray)
          case ObjectType.KeyValue => new KeyValueObjectPointer(newObjectId, poolId, objectSize, objectIDA, sortedPointersArray)
        }
        promise.success(op)
      } else
        promise.failure(AllocationError(poolId))
    }
  }
}

object BaseAllocationDriver {

  object Factory extends AllocationDriver.Factory {
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
               revisionGuard: AllocationRevisionGuard): BaseAllocationDriver = {
      new BaseAllocationDriver(client, poolId, newObjectId, objectSize, objectIDA, objectData, objectType, timestamp,
        initialRefcount, allocationTransactionId, revisionGuard)
    }
  }

  val NoErrorRecoveryAllocationDriver: Factory.type = Factory
}
