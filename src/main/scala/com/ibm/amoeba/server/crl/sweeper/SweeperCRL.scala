package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.crl.{AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogClient, TransactionRecoveryState, TxSaveId}

class SweeperCRL(sweeper: Sweeper,
                 clientId: CrashRecoveryLogClient) extends CrashRecoveryLog {

  override def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState]) = {
    sweeper.getFullRecoveryState(storeId)
  }

  override def save(txid: TransactionId, state: TransactionRecoveryState, saveId: TxSaveId): Unit = {
    sweeper.enqueue(TxSave(clientId, txid, state, saveId))
  }

  override def save(state: AllocationRecoveryState): Unit = {
    sweeper.enqueue(AllocSave(clientId, state))
  }

  override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = {
    DropTxData(TxId(storeId, txid))
  }

  override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = {
    DeleteTx(TxId(storeId, txid))
  }

  override def deleteAllocation(storeId: StoreId, txid: TransactionId): Unit = {
    DeleteAlloc(TxId(storeId, txid))
  }
}
