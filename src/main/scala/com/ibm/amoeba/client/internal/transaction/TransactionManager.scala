package com.ibm.amoeba.client.internal.transaction

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.client.internal.transaction.TransactionBuilder.TransactionData
import com.ibm.amoeba.common.network.{TransactionFinalized, TransactionResolved}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionId}

import scala.concurrent.Future

class TransactionManager(val client: AmoebaClient,
                         defaultDriverFactory: ClientTransactionDriver.Factory) {

  private[this] var transactions = Map[TransactionId, ClientTransactionDriver]()

  def shutdown(): Unit = synchronized {
    transactions.values.foreach(_.shutdown())
  }

  def runTransaction(
                      txd: TransactionDescription,
                      updateData: Map[StoreId, TransactionData],
                      driverFactory: Option[ClientTransactionDriver.Factory]): Future[Boolean] = {
    val td = driverFactory.getOrElse(defaultDriverFactory)(client, txd, updateData)

    synchronized {
      transactions += (txd.transactionId -> td)
    }

    td.begin()

    td.result
  }

  def receive(resolved: TransactionResolved): Unit = {

    val otd = synchronized { transactions.get(resolved.transactionId) }
    otd.foreach { td =>
      td.receive(resolved)
      synchronized { transactions -= resolved.transactionId }
    }
  }
  def receive(finalized: TransactionFinalized): Unit = {
    val otd = synchronized { transactions.get(finalized.transactionId) }
    otd.foreach { td =>
      td.receive(finalized)
      synchronized { transactions -= finalized.transactionId }
    }
  }
}
