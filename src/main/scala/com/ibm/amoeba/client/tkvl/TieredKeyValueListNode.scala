package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.client.KeyValueObjectState.ValueState
import com.ibm.amoeba.client.Transaction
import com.ibm.amoeba.common.objects.{Key, KeyValueObjectPointer, ObjectRevision, Value}

import scala.concurrent.{ExecutionContext, Future}

class TieredKeyValueListNode(val tkvl: TieredKeyValueList,
                             private val node: KeyValueListNode) {

  implicit val ec: ExecutionContext = tkvl.client.clientContext

  def get(key: Key): Option[ValueState] = node.contents.get(key)

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)(implicit t: Transaction): Future[Unit] = {

    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(tkvl.rootManager, 1, newMinimum, newNode, t)
      Future.successful(())
    }

    for {
      alloc <- tkvl.rootManager.getAllocatorForTier(0)
      maxNodeSize <- tkvl.rootManager.getMaxNodeSize(0)

      _ <- node.insert(key, value, maxNodeSize, alloc, onSplit, requirement)
    } yield {
      ()
    }
  }

  def delete(key: Key)(implicit t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(tkvl.rootManager, 1, delMinimum, delNode, t)
      Future.successful(())
    }

    node.delete(key, onJoin)
  }
}
