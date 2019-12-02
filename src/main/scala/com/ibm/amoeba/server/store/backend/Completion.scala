package com.ibm.amoeba.server.store.backend

import com.ibm.amoeba.common.objects.{ObjectId, ReadError}
import com.ibm.amoeba.common.store.{ReadState, StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId

sealed abstract class Completion {
  val storeId: StoreId
}

case class Read(storeId: StoreId,
                objectId: ObjectId,
                storePointer: StorePointer,
                result: Either[ReadState, ReadError.Value]) extends Completion

case class Commit(storeId: StoreId,
                  objectId: ObjectId,
                  transactionId: TransactionId,
                  result: Either[Unit, CommitError.Value]) extends Completion