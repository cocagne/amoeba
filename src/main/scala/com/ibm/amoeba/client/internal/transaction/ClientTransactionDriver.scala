package com.ibm.amoeba.client.internal.transaction

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.client.internal.transaction.TransactionBuilder.TransactionData
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.network.{TransactionFinalized, TransactionResolved, TxPrepare}
import com.ibm.amoeba.common.paxos.ProposalId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionDescription

import scala.concurrent.{Future, Promise}

object ClientTransactionDriver {
  type Factory = (AmoebaClient, TransactionDescription, Map[StoreId, TransactionData]) => ClientTransactionDriver

  def noErrorRecoveryFactory(
                              client: AmoebaClient,
                              txd: TransactionDescription,
                              updateData: Map[StoreId, TransactionData]): ClientTransactionDriver = new ClientTransactionDriver(client, txd, updateData)
}

class ClientTransactionDriver(
                               val client: AmoebaClient,
                               val txd: TransactionDescription,
                               val updateData: Map[StoreId, TransactionData]) {

  //protected val learner = new Learner(txd.primaryObject.ida.width, txd.primaryObject.ida.writeThreshold)
  protected val promise: Promise[Boolean] = Promise()

  def result: Future[Boolean] = promise.future

  def begin(): Unit = sendPrepareMessages()

  def shutdown(): Unit = {}

  private def complete(committed: Boolean): Unit = if (!promise.isCompleted) {
    HLCTimestamp.update(txd.startTimestamp)
    promise.success(committed)
  }

  /*
  def receive(acceptResponse: TxAcceptResponse): Unit = synchronized {
    if (promise.isCompleted)
      return

    acceptResponse.response match {
      case Left(nack) => // Nothing to do
      case Right(accepted) =>
        learner.receiveAccepted(paxos.Accepted(acceptResponse.from.poolIndex, acceptResponse.proposalId, accepted.value)) match {
          case None =>
          case Some(committed) => complete(committed)
        }
    }
  }
  */

  def receive(finalized: TransactionFinalized): Unit = synchronized { complete(finalized.committed) }

  def receive(resolved: TransactionResolved): Unit = synchronized { complete(resolved.committed) }

  protected def sendPrepareMessages(): Unit = {
    val isCompleted = synchronized { promise.isCompleted }

    if (!isCompleted) {
      val poolId = txd.primaryObject.poolId

      val fromStore = StoreId(poolId, txd.designatedLeaderUID)

      txd.allDataStores.foreach { toStore =>

        val transactionData = updateData.getOrElse(toStore, TransactionData(Nil, Nil))

        val initialPrepare = TxPrepare(toStore, fromStore, txd, ProposalId.initialProposal(txd.designatedLeaderUID),
          transactionData.localUpdates, transactionData.preTransactionRebuilds)

        client.messenger.sendTransactionMessage(initialPrepare)
      }
    }
  }
}
