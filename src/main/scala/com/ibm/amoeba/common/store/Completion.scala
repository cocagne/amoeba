package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.transaction.TransactionId

sealed abstract class Completion

case class Read(storeId: StoreId,
                objectId: ObjectId,
                storePointer: StorePointer,
                result: Either[ReadState, ReadError.Value]) extends Completion

case class Commit(storeId: StoreId,
                  objectId: ObjectId,
                  transactionId: TransactionId,
                  result: Either[Unit, CommitError.Value]) extends Completion