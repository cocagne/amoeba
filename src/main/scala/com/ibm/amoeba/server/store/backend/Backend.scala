package com.ibm.amoeba.server.store.backend

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.store.Locater
import org.apache.logging.log4j.scala.Logging

trait Backend extends Logging {
  val storeId: StoreId

  def close(): Unit

  def setCompletionHandler(handler: CompletionHandler): Unit

  /** Bootstrap-only allocation method. It cannot fail and must return a StorePointer to data committed to disk */
  def bootstrapAllocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer): StorePointer

  /** Bootstrap-only overwrite method. It cannot fail and must return after the data is committed to disk */
  def bootstrapOverwrite(objectId: ObjectId, pointer: StorePointer, data:DataBuffer): Unit

  def allocate(objectId: ObjectId,
               objectType: ObjectType.Value,
               metadata: Metadata,
               data: DataBuffer,
               maxSize: Option[Int]): Either[StorePointer, AllocationError.Value]

  def abortAllocation(objectId: ObjectId): Unit

  def read(locater: Locater): Unit

  def commit(state: CommitState, transactionId: TransactionId): Unit
}
