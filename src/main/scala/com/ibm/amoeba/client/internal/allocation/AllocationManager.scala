package com.ibm.amoeba.client.internal.allocation

import java.util.UUID

import com.ibm.amoeba.client.internal.pool.StoragePool
import com.ibm.amoeba.client.{AmoebaClient, Transaction}
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.network.AllocateResponse
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectType}

import scala.concurrent.{ExecutionContext, Future}

class AllocationManager( val client: AmoebaClient,
                         val driverFactory: AllocationDriver.Factory) {

  implicit val ec: ExecutionContext = client.clientContext

  // Maps newObjectUUID -> driver
  private[this] var outstandingAllocations = Map[ObjectId, AllocationDriver]()

  def shutdown(): Unit = outstandingAllocations.foreach( t => t._2.shutdown() )

  def receive(m: AllocateResponse): Unit = {
    synchronized { outstandingAllocations.get(m.newObjectId) } foreach {
      driver => driver.receiveAllocationResult(m.fromStore, m.result)
    }
  }

  private def allocate[PointerType <: ObjectPointer](
                                                      client: AmoebaClient,
                                                      transaction: Transaction,
                                                      pool: StoragePool,
                                                      objectSize: Option[Int],
                                                      objectIDA: IDA,
                                                      encodedContent: Array[DataBuffer],
                                                      objectType: ObjectType.Value,
                                                      initialRefcount: ObjectRefcount,
                                                      revisionGuard: AllocationRevisionGuard
                                                    ): Future[PointerType] = {

    val hosts = pool.selectStoresForAllocation(objectIDA)
    val objectData = hosts.map(_.asInstanceOf[Byte]).zip(encodedContent).toMap
    val newObjectId = ObjectId(UUID.randomUUID())

    val timestamp = HLCTimestamp.now

    val driver = driverFactory.create(client, pool.poolId, newObjectId.uuid, objectSize, objectIDA, objectData, objectType,
      timestamp, initialRefcount, transaction.id.uuid, revisionGuard)

    synchronized { outstandingAllocations += (newObjectId -> driver) }

    driver.futureResult onComplete {
      _ => synchronized { outstandingAllocations -= newObjectId }
    }

    val r = driver.futureResult map { newObjectPtr =>
      // TODO Add Allocation Finalization Action!
      //AllocationFinalizationAction.addToAllocationTree(transaction, pool.poolDefinitionPointer, newObjectPtr)
      newObjectPtr.asInstanceOf[PointerType]
    }

    driver.start()

    r
  }

  def allocateDataObject(
                          client: AmoebaClient,
                          transaction: Transaction,
                          pool: StoragePool,
                          objectSize: Option[Int],
                          objectIDA: IDA,
                          encodedContent: Array[DataBuffer],
                          initialRefcount: ObjectRefcount,
                          revisionGuard: AllocationRevisionGuard): Future[DataObjectPointer] = {

    allocate(client, transaction, pool, objectSize, objectIDA, encodedContent, ObjectType.Data,
      initialRefcount, revisionGuard)
  }

  def allocateKeyValueObject(
                              client: AmoebaClient,
                              transaction: Transaction,
                              pool: StoragePool,
                              objectSize: Option[Int],
                              objectIDA: IDA,
                              initialRefcount: ObjectRefcount,
                              revisionGuard: AllocationRevisionGuard,
                              encodedContent: Array[DataBuffer]): Future[KeyValueObjectPointer] = {

    allocate(client, transaction, pool, objectSize, objectIDA, encodedContent, ObjectType.KeyValue,
      initialRefcount, revisionGuard)
  }
}
