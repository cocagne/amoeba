package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.common.network.ClientId
import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId

sealed abstract class Completion

case class TxSave( clientId: ClientId,
                   storeId: StoreId,
                   transactionId: TransactionId,
                   saveId: TxSaveId) extends Completion

case class AllocSave( clientId: ClientId,
                      transactionId: TransactionId,
                      storeId: StoreId,
                      objectId: ObjectId) extends Completion
