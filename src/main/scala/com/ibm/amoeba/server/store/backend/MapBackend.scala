package com.ibm.amoeba.server.store.backend
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.{ReadState, StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.store.{AllocationError, Locater, ObjectState}

class MapBackend(val storeId: StoreId) extends Backend {

  private var chandler: Option[CompletionHandler] = None
  private var m: Map[ObjectId, ObjectState] = Map()

  override def setCompletionHandler(handler: CompletionHandler): Unit = {
    chandler = Some(handler)
  }

  override def allocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer,
                        maxSize: Option[Int]): Either[StorePointer, AllocationError.Value] = {

    val sp = StorePointer(storeId.poolIndex, Array())
    val os = new ObjectState(objectId, sp, metadata, objectType, data, maxSize)
    m += (objectId -> os)
    Left(sp)
  }

  override def abortAllocation(objectId: ObjectId): Unit = {
    m -= objectId
  }

  override def read(locater: Locater): Unit = {
    m.get(locater.objectId).foreach { os =>
      chandler.foreach { handler =>
        val rs = ReadState(os.objectId, os.metadata, os.objectType, os.data, os.lockedWriteTransactions)
        handler.complete(Read(storeId, locater.objectId, os.storePointer, Left(rs)))
      }
    }
  }

  override def commit(state: CommitState, transactionId: TransactionId): Unit = {
    val os = new ObjectState(objectId = state.objectId,
      storePointer = state.storePointer,
      metadata = state.metadata,
      objectType = state.objectType,
      data = state.data,
      maxSize = m.get(state.objectId).flatMap(os => os.maxSize))
    m += (state.objectId -> os)
  }
}
