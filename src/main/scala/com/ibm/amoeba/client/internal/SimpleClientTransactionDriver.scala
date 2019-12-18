package com.ibm.amoeba.client.internal

import com.ibm.amoeba.client.AmoebaClient
import org.apache.logging.log4j.scala.Logging
import com.ibm.amoeba.client.internal.TransactionBuilder.TransactionData
import com.ibm.amoeba.common.network.{TxAcceptResponse, TxPrepare, TxPrepareResponse}
import com.ibm.amoeba.common.paxos.ProposalId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionDescription
import com.ibm.amoeba.common.util.BackgroundTask

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object SimpleClientTransactionDriver {

  def factory(retransmitDelay: Duration): ClientTransactionDriver.Factory = {
    def f(
           client: AmoebaClient,
           txd: TransactionDescription,
           updateData: Map[StoreId, TransactionData]): ClientTransactionDriver = new SimpleClientTransactionDriver(retransmitDelay, client, txd, updateData)

    f
  }

}

class SimpleClientTransactionDriver(
                                     val retransmitDelay: Duration,
                                     client: AmoebaClient,
                                     txd: TransactionDescription,
                                     updateData: Map[StoreId, TransactionData])
    extends ClientTransactionDriver(client, txd, updateData) with Logging {

  implicit private val ec: ExecutionContext = client.clientContext

  private var haveUpdateContent = Set[StoreId]()
  private var responded = Set[StoreId]()

  private var retries = 0

  private val task = BackgroundTask.schedulePeriodic(retransmitDelay) {
    synchronized {
      retries += 1
      if (retries % 3 == 0)
        logger.info(s"***** HUNG Client Transaction ${txd.transactionId}")
    }
    retransmit()
  }

  override def shutdown(): Unit = task.cancel()

  result onComplete { _ => task.cancel() }

  override def receive(prepareResponse: TxPrepareResponse): Unit = synchronized {
    haveUpdateContent += prepareResponse.from
  }

  override def receive(acceptResponse: TxAcceptResponse): Unit = synchronized {
    responded += acceptResponse.from
    super.receive(acceptResponse)
  }

  def retransmit(): Unit = synchronized {

    val poolId = txd.primaryObject.poolId
    val fromStore = StoreId(poolId, txd.designatedLeaderUID)
    val pid = ProposalId.initialProposal(txd.designatedLeaderUID)

    txd.allDataStores.filter(!responded.contains(_)).foreach { toStore =>

      val transactionData = updateData.getOrElse(toStore, TransactionData(Nil, Nil))

      val initialPrepare = TxPrepare(toStore, fromStore, txd, pid,
        transactionData.localUpdates, transactionData.preTransactionRebuilds)

      client.messenger.sendTransactionMessage(initialPrepare)
    }
  }

}
