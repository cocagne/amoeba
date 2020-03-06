package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.client.{AmoebaClient, Transaction}
import com.ibm.amoeba.client.KeyValueObjectState.ValueState
import com.ibm.amoeba.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectId, Value}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class TieredKeyValueList(val client: AmoebaClient,
                         val rootManager: RootManager) {

  implicit val ec: ExecutionContext = client.clientContext

  import TieredKeyValueList._

  def get(key: Key): Future[Option[ValueState]] = {
    rootManager.getRootNode().flatMap { t =>
      val (tier, ordering, node) = t
      fetchContainingNode(client, tier, 0, ordering, key, node, Set()).map {
        case Left(_) => throw new BrokenTree()
        case Right(node) => node.contents.get(key)
      }
    }
  }

  def set(key: Key, value: Value)(implicit t: Transaction): Future[Unit] = {
    def onSplit(newMinimum: Key, newNode: KeyValueObjectPointer): Future[Unit] = {
      SplitFinalizationAction.addToTransaction(rootManager, 1, newMinimum, newNode, t)
      Future.successful(())
    }
    for {
      (tier, ordering, root) <- rootManager.getRootNode()
      alloc <- rootManager.getAllocatorForTier(0)
      maxNodeSize <- rootManager.getMaxNodeSize(0)
      e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
      node = e match {
        case Left(_) => throw new BrokenTree()
        case Right(n) => n
      }
      _ <- node.insert(key, value, maxNodeSize, alloc, onSplit)
    } yield {
      ()
    }
  }

  def delete(key: Key)(implicit t: Transaction): Future[Unit] = {
    def onJoin(delMinimum: Key, delNode: KeyValueObjectPointer): Future[Unit] = {
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.successful(())
    }
    for {
      (tier, ordering, root) <- rootManager.getRootNode()
      e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
      node = e match {
        case Left(_) => throw new BrokenTree()
        case Right(n) => n
      }
      _ <- node.delete(key, onJoin)
    } yield {
      ()
    }
  }
}

object TieredKeyValueList {

  private[tkvl] def fetchContainingNode(client: AmoebaClient,
                          currentTier: Int,
                          targetTier: Int,
                          ordering: KeyOrdering,
                          target: Key,
                          currentNode: KeyValueListNode,
                          initialBlacklist: Set[ObjectId]): Future[Either[Set[ObjectId], KeyValueListNode]] = {

    implicit val ec: ExecutionContext = client.clientContext

    if (currentTier == targetTier) {
      // Once we're on the right tier, we can rely on consistent right pointers to scan to the
      // containing node
      currentNode.fetchContainingNode(target).map(n => Right(n))
    } else {

      val p = Promise[Either[Set[ObjectId], KeyValueListNode]]()

      def rtry(candidates: List[(Key, KeyValueObjectPointer)], blacklist: Set[ObjectId]): Unit = {
        if (candidates.isEmpty) {
          p.success(Left(blacklist + currentNode.pointer.id))
        } else {
          fetchNode(client, ordering, candidates.head._1, candidates.head._2, blacklist) foreach {
            case Left(blklst) => rtry(candidates.tail, blklst)
            case Right(next) => fetchContainingNode(client, currentTier-1, targetTier, ordering, target,
              next, blacklist).foreach {
              case Left(blklst) => p.success(Left(blklst + next.pointer.id))
              case Right(targetNode) => p.success(Right(targetNode))
            }
          }
        }
      }

      val initialCandidates = currentNode.contents.iterator.
        filter(t => ordering.compare(t._1, target) <= 0).
        map(t => t._1 -> KeyValueObjectPointer(t._2.value.bytes)).
        filter(t => !initialBlacklist.contains(t._2.id)).
        toList.
        sortWith((l, r) => ordering.compare(l._1, r._1) < 0).
        reverse

      rtry(initialCandidates, initialBlacklist)

      p.future
    }

  }

  private def fetchNode(client: AmoebaClient,
                        ordering: KeyOrdering,
                        minimum: Key,
                        pointer: KeyValueObjectPointer,
                        blacklist: Set[ObjectId]): Future[Either[Set[ObjectId], KeyValueListNode]] = {

    implicit val ec: ExecutionContext = client.clientContext

    if (blacklist.contains(pointer.id)) {
      Future.successful(Left(blacklist))
    } else {
      val p = Promise[Either[Set[ObjectId], KeyValueListNode]]()

      client.read(pointer) onComplete {
        case Failure(_) => p.success(Left(blacklist + pointer.id))

        case Success(kvos) =>

          val tail = kvos.right.map(right => KeyValueListPointer(right.bytes))

          val node = new KeyValueListNode(client, pointer, ordering, minimum,
            kvos.revision, kvos.refcount, kvos.contents, tail)
          p.success(Right(node))
      }

      p.future
    }
  }

}