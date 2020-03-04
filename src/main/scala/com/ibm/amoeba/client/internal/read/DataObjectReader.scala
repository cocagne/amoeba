package com.ibm.amoeba.client.internal.read

import java.util.UUID

import com.ibm.amoeba.client.{DataObjectState, ObjectState}
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.network.ReadResponse
import com.ibm.amoeba.common.objects.{DataObjectPointer, ObjectRefcount, ObjectRevision}
import com.ibm.amoeba.common.store.StoreId

class DataObjectReader(metadataOnly: Boolean, pointer: DataObjectPointer, readUUID: UUID)
  extends BaseObjectReader[DataObjectPointer, DataObjectStoreState](metadataOnly, pointer, readUUID) {

  override protected def createObjectState(storeId:StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): DataObjectStoreState = {
    new DataObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.sizeOnStore, cs.objectData)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, matchingStoreStates: List[DataObjectStoreState],
                                       allStoreStates: List[DataObjectStoreState], debug: Boolean): ObjectState = {

    val sizeOnStore = matchingStoreStates.head.sizeOnStore

    val segments = matchingStoreStates.foldLeft(List[(Byte,DataBuffer)]()) { (l, ss) => ss.objectData match {
      case None => l
      case Some(db) => (ss.storeId.poolIndex -> db) :: l
    }}

    if (segments.size >= threshold) {
      val data = pointer.ida.restore(segments)
      val obj = DataObjectState(pointer, revision, refcount, timestamp, readTime, sizeOnStore, data)
      obj
    }
    else
      throw BaseObjectReader.NotRestorable(s"Below Threshold")
  }
}
