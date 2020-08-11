package com.ibm.amoeba.client.internal.transaction

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.client.internal.transaction.TransactionBuilder.TransactionData
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionDescription
import org.apache.logging.log4j.scala.Logging

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

  private var retries = 0
  private var complete = false

  private val task = client.backgroundTasks.schedulePeriodic(retransmitDelay) {
    val send = synchronized {
      if (complete)
        false
      else {
        retries += 1
        if (retries % 3 == 0)
          logger.info(s"***** HUNG Client Transaction ${txd.transactionId}")
        true
      }
    }
    if (send)
      sendPrepareMessages()
  }

  override def shutdown(): Unit = task.cancel()

  override protected def complete(committed: Boolean): Unit = synchronized {
    task.cancel()
    complete = true
    super.complete(committed)
  }
}
