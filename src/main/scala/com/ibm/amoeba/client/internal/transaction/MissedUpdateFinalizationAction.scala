package com.ibm.amoeba.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, StopRetrying, Transaction, UnknownStoragePool}
import com.ibm.amoeba.common.objects.{Key, ObjectId, ObjectPointer, Value}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{FinalizationActionId, SerializedFinalizationAction}
import com.ibm.amoeba.common.util.BackgroundTask.ScheduledTask

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class MissedUpdateFinalizationAction(val client: AmoebaClient) extends FinalizationAction {

  implicit val ec: ExecutionContext = client.clientContext

  private var commitErrors: Map[StoreId, List[ObjectId]] = Map()
  private var haveErrors: Boolean = true

  private var markTask: Option[ScheduledTask] = None

  private val completionPromise: Promise[Unit] = Promise()

  logger.debug(s"Created MissedUpdateFinalizationAction")

  def complete: Future[Unit] = completionPromise.future

  override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = synchronized {
    this.commitErrors = commitErrors

    if (commitErrors.isEmpty && haveErrors) {
      haveErrors = false
      markTask.foreach(_.cancel())
      if (!completionPromise.isCompleted)
        completionPromise.success(())
    }
  }

  def execute(): Unit = synchronized {
    if (haveErrors) {
      markTask = Some(client.backgroundTasks.schedule(MissedUpdateFinalizationAction.errorTimeout) {
        markMissedUpdates()
      })
    }
  }

  def markMissedUpdates(): Unit = synchronized {
    if (haveErrors) {
      val misses = commitErrors.foldLeft(List[(StoreId, ObjectId)]()){ (l, t) =>
        t._2.map(obj => t._1 -> obj) ++ l
      }
      Future.sequence(misses.map(t => markMissedUpdate(t._1, t._2))).foreach { _ =>
        synchronized {
          if (!completionPromise.isCompleted)
            completionPromise.success(())
        }
      }
    }
  }

  def markMissedUpdate(storeId: StoreId, objectId: ObjectId): Future[Unit] = {
    def onFail(err: Throwable): Future[Unit] = { err match {
      case err: UnknownStoragePool =>
        // StoragePool must have been deleted
        throw StopRetrying(err)

      case _ => Future.unit
    }}

    val keyBytes = new Array[Byte](17)
    val bb = ByteBuffer.wrap(keyBytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)

    val key = Key(keyBytes)

    client.retryStrategy.retryUntilSuccessful(onFail _) {
      for {
        pool <- client.getStoragePool(storeId.poolId)
        tx = client.newTransaction()
        _ = tx.disableMissedUpdateTracking()
        _ <- pool.errorTree.set(key, Value(Array()))(tx)
        _ <- tx.commit()
      } yield {
        ()
      }
    }
  }
}

object MissedUpdateFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("E1F61997-61A7-4C86-A211-0EBA601DEDA3")

  var errorTimeout = Duration(250, MILLISECONDS)

  def createFinalizationAction(client: AmoebaClient, data: Array[Byte]): FinalizationAction = {

    new MissedUpdateFinalizationAction(client)
  }

  def createSerializedFA(missedCommitDelayInMs: Int): SerializedFinalizationAction = {
    SerializedFinalizationAction(FinalizationActionId(typeUUID), Array[Byte]())
  }

}