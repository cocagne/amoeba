package com.ibm.amoeba.server.crl

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.paxos.PersistentState
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDescription, TransactionDisposition, TransactionStatus}

case class TransactionRecoveryState( storeId: StoreId,
                                     serializedTxd: DataBuffer,
                                     objectUpdates: List[ObjectUpdate],
                                     disposition: TransactionDisposition.Value,
                                     status: TransactionStatus.Value,
                                     paxosAcceptorState: PersistentState):
  def txd: TransactionDescription = TransactionDescription.deserialize(serializedTxd)

object TransactionRecoveryState {
  def initial(storeId: StoreId,
              txd: TransactionDescription,
              objectUpdates: List[ObjectUpdate]): TransactionRecoveryState = {


    val stxd = txd.serialize()

    TransactionRecoveryState(storeId, stxd, objectUpdates, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, PersistentState.initial)
  }
}
