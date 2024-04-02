package com.ibm.amoeba.server.crl.simple

import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionId

final case class TxId(storeId: StoreId, transactionId: TransactionId):

  override def equals(other: Any): Boolean = {
    other match {
      case that: TxId => that.storeId == this.storeId && that.transactionId == this.transactionId
      case _ => false
    }
  }

  override def hashCode: Int = (storeId, transactionId).##

object TxId:
  /// store::Id + UUID
  val StaticSize: Int = 17 + 16

