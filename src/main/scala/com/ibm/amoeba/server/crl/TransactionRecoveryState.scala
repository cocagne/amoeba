package com.ibm.amoeba.server.crl

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.paxos.PersistentState
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDisposition, TransactionStatus}

case class TransactionRecoveryState( storeId: StoreId,
                                     serializedTxd: DataBuffer,
                                     objectUpdates: List[ObjectUpdate],
                                     disposition: TransactionDisposition.Value,
                                     status: TransactionStatus.Value,
                                     paxosAcceptorState: PersistentState)
