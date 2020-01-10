package com.ibm.amoeba.client.internal.allocation

import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator, StoragePool, Transaction}
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, DataObjectPointer, Key, KeyValueObjectPointer, ObjectRefcount, Value}

import scala.concurrent.Future

class SinglePoolObjectAllocator(val client: AmoebaClient,
                                val pool: StoragePool,
                                val objectIDA: IDA,
                                val maxObjectSize: Option[Int]) extends ObjectAllocator {


  override def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                                  initialContent: DataBuffer,
                                  initialRefcount: ObjectRefcount)(implicit t: Transaction): Future[DataObjectPointer] = {
    client.allocationManager.allocateDataObject(client, t, pool, maxObjectSize, objectIDA, initialRefcount,
      revisionGuard, initialContent)
  }

  override def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                                      initialContent: Map[Key, Value],
                                      minimum: Option[Key],
                                      maximum: Option[Key],
                                      left: Option[Value],
                                      right: Option[Value],
                                      initialRefcount: ObjectRefcount)(implicit t: Transaction): Future[KeyValueObjectPointer] = {
    client.allocationManager.allocateKeyValueObject(client, t, pool, maxObjectSize, objectIDA, initialRefcount,
      revisionGuard, initialContent, minimum, maximum, left, right)
  }
}
