package com.ibm.amoeba.server.store

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.StorePointer
import com.ibm.amoeba.common.transaction.TransactionId

class ObjectState(val objectId: ObjectId,
                  val storePointer: StorePointer,
                  var metadata: Metadata,
                  val objectType: ObjectType.Value,
                  var data: DataBuffer,
                  val maxSize: Option[Int]) {

  /** Used to track the number of references currently working on this object.
    * The object is not allowed to exit the cache until this number drops to
    * zero.
    */
  var transactionReferences: Int = 0

  var lockedToTransaction: Option[TransactionId] = None

  var kvState: Option[KVObjectState] = None

}
