package com.ibm.amoeba.server.store

import java.util.UUID

import com.ibm.amoeba.common.{HLCTimestamp, Nucleus}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{KeyValueObjectPointer, KeyValueOperation, Metadata, ObjectRefcount, ObjectRevision}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.store.backend.Backend

object Bootstrap {

    def initialize(ida: IDA, stores: List[Backend]): KeyValueObjectPointer = {
      require( ida.width == stores.length )

      val nucleusOps: List[KeyValueOperation] = List()

      val nucleusMetadata = Metadata(
        ObjectRevision(TransactionId(new UUID(0,0))),
        ObjectRefcount(1,1),
        HLCTimestamp.now
      )

      val nucleusStorePointers = KeyValueOperation.encode(nucleusOps, ida).zip(stores).map { t =>
        val (storeData, store) = t

        store.bootstrapAllocate(Nucleus.objectId, Nucleus.objectType, nucleusMetadata, storeData)
      }

      KeyValueObjectPointer(Nucleus.objectId, Nucleus.poolId, None, ida, nucleusStorePointers)
    }

}
