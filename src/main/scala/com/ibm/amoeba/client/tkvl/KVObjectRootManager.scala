package com.ibm.amoeba.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator, RegisteredTypeFactory, Transaction}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, Insert, Key, KeyOrdering, KeyRevisionGuard, KeyValueObjectPointer, ObjectRevision, Value}
import com.ibm.amoeba.common.transaction.KeyValueUpdate

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class KVObjectRootManager(val client: AmoebaClient,
                          val treeKey: Key,
                          val pointer: KeyValueObjectPointer) extends RootManager {

  import KVObjectRootManager._

  implicit val ec: ExecutionContext = client.clientContext

  def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  /** Returns (numTiers, keyOrdering, rootNode) */
  private def getRoot(): Future[RData] = {
    val p = Promise[RData]()

    client.read(pointer).onComplete {
      case Failure(err) => p.failure(err)
      case Success(container) =>

        container.contents.get(treeKey) match {
          case None => p.failure(new InvalidRoot)
          case Some(v) => try {
            val root = Root(client, v.value.bytes)

            client.read(root.rootObject).onComplete {
              case Failure(err) => p.failure(err)
              case Success(rootKvos) =>

                val rootLp = KeyValueListPointer(Key.AbsoluteMinimum, root.rootObject)
                val node = KeyValueListNode(client, rootLp, root.ordering, rootKvos)

                p.success(RData(root, v.revision, node))
            }
          } catch {
            case _: Throwable => p.failure(new InvalidRoot)
          }
        }
    }

    p.future
  }

  def getTree(): Future[TieredKeyValueList] = getRoot().map { rd =>
    new TieredKeyValueList(client, this)
  }

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = getRoot().flatMap { rd =>
    rd.root.nodeAllocator.getAllocatorForTier(tier)
  }

  def getRootNode(): Future[(Int, KeyOrdering, KeyValueListNode)] = getRoot().map { rd =>
    (rd.root.tier, rd.root.ordering, rd.node)
  }

  def getMaxNodeSize(tier: Int): Future[Int] = getRoot().map { rd =>
    rd.root.nodeAllocator.getMaxNodeSize(tier)
  }

  def encode(): Array[Byte] = {
    val arr = new Array[Byte]( 4 + treeKey.bytes.length + pointer.encodedSize )
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(treeKey.bytes.length)
    bb.put(treeKey.bytes)
    pointer.encodeInto(bb)
    arr
  }

  def prepareRootUpdate(newTier: Int, newRoot: KeyValueObjectPointer)(implicit tx: Transaction): Future[Unit] = {
    getRoot().map { rd =>
      if (rd.root.tier != newTier) {
        val data = rd.root.copy(tier=newTier, rootObject=newRoot).encode()

        val reqs = KeyValueUpdate.KeyRevision(treeKey, rd.rootRevision) :: Nil
        val ops = Insert(treeKey, data) :: Nil

        tx.update(pointer, None, None, reqs, ops)
      }
    }
  }

  def getRootRevisionGuard(): Future[AllocationRevisionGuard] = {

    getRoot().map { rd =>
      KeyRevisionGuard(rd.node.pointer, treeKey, Some(rd.rootRevision))
    }
  }
}

object KVObjectRootManager extends RegisteredTypeFactory with RootManagerFactory {

  private case class RData(root: Root, rootRevision: ObjectRevision, node: KeyValueListNode)

  val typeUUID: UUID = UUID.fromString("CE36789D-42F1-43F9-9464-E9B44419D8C4")

  def createRootManager(client: AmoebaClient, data: Array[Byte]): KVObjectRootManager = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val klen = bb.getInt()
    val karr = new Array[Byte](klen)
    bb.get(karr)
    val ptr = KeyValueObjectPointer(bb)
    new KVObjectRootManager(client, Key(karr), ptr)
  }

  /** Outter future is for ready to commit transaction. Inner future is to the root manager after successful
    * transaction completion.
    *
    * Note that the key must not already exist within the object
    */
  def createNewTree(client: AmoebaClient,
                    pointer: KeyValueObjectPointer,
                    key: Key,
                    ordering: KeyOrdering,
                    nodeAllocator: NodeAllocator,
                    initialContent: Map[Key,Value])(implicit tx: Transaction): Future[Future[KVObjectRootManager]] = {

    implicit val ec: ExecutionContext = client.clientContext

    val kreqs = KeyValueUpdate.DoesNotExist(key) :: Nil

    for {
      alloc <- nodeAllocator.getAllocatorForTier(0)
      rptr <- alloc.allocateKeyValueObject(KeyRevisionGuard(pointer, key, None), initialContent)
    } yield {
      val root = Root(0, ordering, rptr, nodeAllocator)
      tx.update(pointer, None, None, kreqs, Insert(key, root.encode()) :: Nil)
      tx.result.map { _ =>
        new KVObjectRootManager(client, key, pointer)
      }
    }
  }
}