package com.ibm.amoeba.server.crl

import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId

trait CrashRecoveryLog {
  def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState])

  def save(txid: TransactionId, state: TransactionRecoveryState, saveId: TxSaveId): Unit

  def save(state: AllocationRecoveryState): Unit

  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit

  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit

  def deleteAllocation(storeId: StoreId, txid: TransactionId): Unit
}
