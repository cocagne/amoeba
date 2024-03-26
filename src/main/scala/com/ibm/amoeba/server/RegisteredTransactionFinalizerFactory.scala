package com.ibm.amoeba.server

import com.ibm.amoeba.client.{AmoebaClient, FinalizationAction, FinalizationActionFactory}
import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionDescription
import com.ibm.amoeba.server
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.transaction.TransactionFinalizer

import scala.concurrent.{ExecutionContext, Future}

object RegisteredTransactionFinalizerFactory {
  class Finalizer(val client: AmoebaClient, val actions: List[FinalizationAction]) extends TransactionFinalizer {
    implicit val ec: ExecutionContext = client.clientContext

    actions.foreach(_.execute())

    def complete: Future[Unit] = Future.sequence(actions.map(_.complete)).map(_=>())

    def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = {
      actions.foreach(_.updateCommitErrors(commitErrors))
    }

    def debugStatus: List[(String, Boolean)] = {
      actions.map(f => (f.getClass.getSimpleName, f.complete.isCompleted))
    }

    def cancel(): Unit = actions.foreach(_.cancel())
  }
}

class RegisteredTransactionFinalizerFactory(val client: AmoebaClient) extends TransactionFinalizer.Factory {
  def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = {
    val actions = txd.finalizationActions.map { sfa =>
      val f = client.typeRegistry.getType[FinalizationActionFactory](sfa.typeId.uuid).get
      f.createFinalizationAction(client, txd, sfa.data)
    }

    new server.RegisteredTransactionFinalizerFactory.Finalizer(client, actions)
  }
}
