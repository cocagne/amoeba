package com.ibm.amoeba.server.crl.sweeper

import java.util.concurrent.LinkedBlockingQueue

import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.crl.{AllocationRecoveryState, CrashRecoveryLogClient, TransactionRecoveryState, TxSaveId}


sealed abstract class Request

case class TxSave(client: CrashRecoveryLogClient,
                  transactionId: TransactionId,
                  state: TransactionRecoveryState,
                  saveId: TxSaveId) extends Request

case class AllocSave(client: CrashRecoveryLogClient,
                     state: AllocationRecoveryState) extends Request

case class DropTxData(txid: TxId) extends Request

case class DeleteTx(txid: TxId) extends Request

case class DeleteAlloc(txid: TxId) extends Request

case class GetFullStoreState(
   storeId: StoreId,
   response: LinkedBlockingQueue[(List[TransactionRecoveryState], List[AllocationRecoveryState]) ]) extends Request

case class ExitIOThread() extends Request