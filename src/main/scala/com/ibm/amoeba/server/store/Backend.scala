package com.ibm.amoeba.server.store

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.StorePointer
import com.ibm.amoeba.common.transaction.TransactionId

trait Backend {
  def setCompletionHandler(handler: CompletionHandler)

  def allocate(objectId: ObjectId,
               objectType: ObjectType.Value,
               metadata: Metadata,
               data: DataBuffer,
               maxSize: Option[Int]): Either[StorePointer, AllocationError.Value]

  def abortAllocation(objectId: ObjectId)

  def read(locater: Locater)

  def commit(state: CommitState, transactionId: TransactionId)
}
