package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.client.KeyValueObjectState.ValueState
import com.ibm.amoeba.client.{KeyValueObjectState, ObjectAllocator, ObjectReader, Transaction}
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.objects._
import com.ibm.amoeba.common.transaction.KeyValueUpdate
import com.ibm.amoeba.server.store.KVObjectState

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class KeyValueListNode(val reader: ObjectReader,
                       val pointer: KeyValueObjectPointer,
                       val ordering: KeyOrdering,
                       val minimum: Key,
                       val revision: ObjectRevision,
                       val contents: Map[Key, ValueState],
                       val tail: Option[KeyValueListPointer]) {

  implicit val ec: ExecutionContext = reader.client.clientContext

  def maximum: Option[Key] = tail.map(lp => lp.minimum)

  def refresh(): Future[KeyValueListNode] = reader.read(pointer).map { kvos =>
    new KeyValueListNode(reader, pointer, ordering, minimum,
      kvos.revision, kvos.contents, kvos.right.map(v => KeyValueListPointer(v.bytes)))
  }

  def keyInRange(key: Key): Boolean = {
    ordering.compare(key, minimum) >= 0 && maximum.forall( ordering.compare(key, _) < 0 )
  }

  def fetchContainingNode(target: Key,
                          blacklist: Set[ObjectId] = Set()): Future[KeyValueListNode] = {
    // exit immediately if the requested key is below the minimum range
    if (ordering.compare(target, minimum) < 0)
      return Future.failed(new BelowMinimumError(minimum, target))

    val p = Promise[KeyValueListNode]()

    def scan(right: KeyValueListPointer): Unit = reader.read(right.pointer) onComplete {
      case Failure(err) => p.failure(err)

      case Success(kvos) =>
        val node = new KeyValueListNode(reader, right.pointer, ordering, right.minimum,
          kvos.revision, kvos.contents, kvos.right.map(v => KeyValueListPointer(v.bytes)))

        node.tail match {
          case None => p.success(node)
          case Some(next) =>
            if (node.keyInRange(target) || blacklist.contains(next.pointer.id))
              p.success(node)
            else
              scan(next)
        }
    }

    tail match {
      case None => p.success(this)
      case Some(ptr) =>
        if (keyInRange(target) || blacklist.contains(ptr.pointer.id))
          p.success(this)
        else
          scan(ptr)
    }

    p.future
  }

  def fetch(key: Key): Future[Option[ValueState]] = fetchContainingNode(key).map { node =>
    node.contents.get(key)
  }

  def insert(key: Key,
             value: Value,
             maxNodeSize: Int,
             allocator: ObjectAllocator)(implicit tx: Transaction): Future[Unit] = fetchContainingNode(key).flatMap { node =>
    KeyValueListNode.insert(node, ordering, key, value, maxNodeSize, allocator)
  }

  def delete(key: Key)(implicit tx: Transaction): Future[Unit] = fetchContainingNode(key).flatMap { node =>
    KeyValueListNode.delete(node, key, reader)
  }
}

object KeyValueListNode {
  def apply(reader: ObjectReader,
            pointer: KeyValueListPointer,
            ordering: KeyOrdering,
            kvos: KeyValueObjectState): KeyValueListNode = {
    new KeyValueListNode(reader, pointer.pointer, ordering, pointer.minimum, kvos.revision,
      kvos.contents, kvos.right.map(v => KeyValueListPointer(v.bytes)))
  }

  // Implemented as a non-class member to prevent accidental use of member variables instead of "node." attributes
  private def insert(node: KeyValueListNode,
                     ordering: KeyOrdering,
                     key: Key,
                     value: Value,
                     maxNodeSize: Int,
                     allocator: ObjectAllocator)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] =  {

    val currentSize = node.contents.foldLeft(0) { (sz, t) =>
      sz + KVObjectState.idaEncodedPairSize(node.pointer.ida, t._1, t._2.value)
    }

    val newPairSize = KVObjectState.idaEncodedPairSize(node.pointer.ida, key, value)

    val maxSize = maxNodeSize - 4 - 4 - node.minimum.bytes.length - node.maximum.map(_.bytes.length).getOrElse(0) - node.tail.map{ p =>
      p.encodedSize
    }.getOrElse(0)

    if (newPairSize > maxSize)
      throw new NodeSizeExceeded

    if (newPairSize + currentSize < maxSize) {
      tx.update(node.pointer, Some(node.revision), Nil, List(Insert(key, value.bytes)))
      Future.successful(())
    } else {
      val fullContent = node.contents + (key -> ValueState(value, tx.revision, HLCTimestamp.now))
      val keys = fullContent.keysIterator.toArray

      scala.util.Sorting.quickSort(keys)(ordering)

      val sizes = keys.iterator.map { k =>
        val vs = fullContent(k)
        (k, KVObjectState.idaEncodedPairSize(node.pointer.ida, k, vs.value))
      }.toList

      val fullSize = sizes.foldLeft(0)((sz, t) => sz + t._2)

      val halfSize = fullSize / 2

      def rmove(rsizes: List[(Key,Int)], moveList: List[Key], moveSize: Int): List[Key] = {
        val (k, sz) = rsizes.head
        if (moveSize + sz >= halfSize)
          moveList
        else
          rmove(rsizes.tail, k :: moveList, moveSize + sz)
      }

      val rsizes = sizes.reverse
      val moveList = rmove(rsizes.tail, rsizes.head._1 :: Nil, rsizes.head._2) // Ensure at least 1 move
      val moves = moveList.toSet

      val deletes: List[Delete] = moveList.map(Delete(_))

      val oldOps = if (moves.contains(key))
        deletes
      else
        Insert(key, value.bytes) :: deletes

      val newMinimum = moveList.head

      val newContent = moveList.map(k => k -> fullContent(k).value).toMap

      allocator.allocateKeyValueObject(ObjectRevisionGuard(node.pointer, node.revision), newContent,
        Some(newMinimum), None, None, node.tail.map(p => Value(p.toArray))).map { newObjectPointer =>

        val rightPtr = KeyValueListPointer(newMinimum, newObjectPointer)

        tx.update(node.pointer, Some(node.revision), List(), SetRight(rightPtr.toArray) :: oldOps)
      }
    }
  }

  def delete(node: KeyValueListNode,
             key: Key,
             reader: ObjectReader)(implicit tx: Transaction, ec: ExecutionContext): Future[Unit] = {
    if (! node.contents.contains(key))
      Future.successful(())
    else {
      if (node.contents.size > 1) {
        tx.update(node.pointer, None, List(KeyValueUpdate.Exists(key)), List(Delete(key)))
        Future.successful(())
      } else {
        node.tail match {
          case None =>
            tx.update(node.pointer, None, List(KeyValueUpdate.Exists(key)), List(Delete(key)))
            Future.successful(())
          case Some(lp) =>
            reader.read(lp.pointer).map { kvos =>
              var ops: List[KeyValueOperation] = Delete(key) :: Nil
              kvos.right match {
                case None => ops = DeleteRight() :: ops
                case Some(rp) => ops = SetRight(rp.bytes) :: ops
              }
              kvos.contents.foreach{ t =>
                ops = Insert(t._1, t._2.value.bytes) :: ops
              }
              tx.update(node.pointer, Some(node.revision), List(), ops)
              tx.setRefcount(lp.pointer, kvos.refcount, kvos.refcount.decrement())
            }
        }
      }
    }
  }
}