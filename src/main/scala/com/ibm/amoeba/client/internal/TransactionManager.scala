package com.ibm.amoeba.client.internal

import com.ibm.amoeba.client.internal.TransactionBuilder.TransactionData
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionDescription

import scala.concurrent.Future

class TransactionManager {

  def runTransaction(
                      txd: TransactionDescription,
                      updateData: Map[StoreId, TransactionData],
                      driverFactory: Option[ClientTransactionDriver.Factory]): Future[Boolean] = ???
}
