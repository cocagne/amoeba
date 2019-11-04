package com.ibm.amoeba.server.crl

import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId

sealed abstract class SaveCompletion

case class TxSaveComplete( clientId: CrashRecoveryLogClient,
                           storeId: StoreId,
                           transactionId: TransactionId,
                           saveId: TxSaveId) extends SaveCompletion

case class AllocSaveComplete( clientId: CrashRecoveryLogClient,
                              transactionId: TransactionId,
                              storeId: StoreId,
                              objectId: ObjectId) extends SaveCompletion
