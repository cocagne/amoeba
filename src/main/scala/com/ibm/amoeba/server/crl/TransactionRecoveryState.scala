package com.ibm.amoeba.server.crl

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.paxos.PersistentState
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDescription, TransactionDisposition, TransactionStatus}
import com.ibm.aspen.core.network.NetworkCodec

case class TransactionRecoveryState( storeId: StoreId,
                                     serializedTxd: DataBuffer,
                                     objectUpdates: List[ObjectUpdate],
                                     disposition: TransactionDisposition.Value,
                                     status: TransactionStatus.Value,
                                     paxosAcceptorState: PersistentState)

object TransactionRecoveryState {
  def initial(storeId: StoreId,
              txd: TransactionDescription,
              objectUpdates: List[ObjectUpdate]): TransactionRecoveryState = {


    val stxd = NetworkCodec.encode(txd)

    TransactionRecoveryState(storeId, stxd, objectUpdates, TransactionDisposition.Undetermined,
      TransactionStatus.Unresolved, PersistentState.initial)
  }
}
