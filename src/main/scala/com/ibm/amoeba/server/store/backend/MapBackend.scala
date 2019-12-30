package com.ibm.amoeba.server.store.backend
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.{ReadState, StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.store.{Locater, ObjectState}

class MapBackend(val storeId: StoreId) extends Backend {

  private var chandler: Option[CompletionHandler] = None

  var m: Map[ObjectId, ObjectState] = Map()

  override def setCompletionHandler(handler: CompletionHandler): Unit = {
    chandler = Some(handler)
  }

  override def bootstrapAllocate(objectId: ObjectId,
                                 objectType: ObjectType.Value,
                                 metadata: Metadata,
                                 data: DataBuffer): StorePointer = {
    val sp = StorePointer(storeId.poolIndex, Array())
    val os = new ObjectState(objectId, sp, metadata, objectType, data, None)

    m += (objectId -> os)

    sp
  }

  override def allocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer,
                        maxSize: Option[Int]): Either[StorePointer, AllocationError.Value] = {

    val sp = StorePointer(storeId.poolIndex, Array())
    val os = new ObjectState(objectId, sp, metadata, objectType, data, maxSize)
    // Do not add to store during allocation since not all store implementations will be
    // able to do so.
    //m += (objectId -> os)
    Left(sp)
  }

  override def abortAllocation(objectId: ObjectId): Unit = {
    //m -= objectId
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
      maxSize = state.maxSize)
    m += (state.objectId -> os)
    chandler.foreach { handler =>
      handler.complete(Commit(storeId, state.objectId, transactionId, Left(())))
    }
  }

  def get(objectId: ObjectId): Option[ObjectState] = m.get(objectId)
}
