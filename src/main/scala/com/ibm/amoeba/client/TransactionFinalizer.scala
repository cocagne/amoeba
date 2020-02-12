package com.ibm.amoeba.client

import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.StoreId

import scala.concurrent.{ExecutionContext, Future, Promise}

class TransactionFinalizer(client: AmoebaClient,
                           actions: List[FinalizationAction]) {

  implicit val ec: ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  completionPromise.completeWith(Future.sequence(actions.map(_.complete)).map(_=>()))

  actions.foreach(_.execute())

  def complete: Future[Unit] = completionPromise.future

  /** Called each time an TxCommitted message is received from a new peer. The provided map contains the
    * aggregated content from all previously-received TxCommitted messages. The list of UUIDs contains the
    * object UUIDs that could not be updated as part of the transaction due to transaction requirement
    * mismatches. Mostly likely, the object copy/slices on those stores will need to be repaired.
    *
    * Finalizers are created immediately upon reaching the threshold commit requirement. Finalizers
    * that depend on knowing which peers have successfully processed the transaction (as indicated by
    * them sending Accepted messages) may delay action for some time to receive additional notifications
    * via this callback.
    */
  def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = {
    actions.foreach(_.updateCommitErrors(commitErrors))
  }

  /**
    * @return List of (test-class-name, is-complete)
    */
  def debugStatus: List[(String, Boolean)] = actions.map(_.debugStatus)

  /** Called when a TxFinalized message is received. */
  def cancel(): Unit = actions.foreach(_.cancel())

}
