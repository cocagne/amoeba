package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.client.{ObjectAllocator, Transaction}
import com.ibm.amoeba.client.tkvl.{RootManager, TieredKeyValueList}
import com.ibm.amoeba.common.objects.Key
import com.ibm.amoeba.common.transaction.KeyValueUpdate
import com.ibm.amoeba.common.transaction.KeyValueUpdate.DoesNotExist
import com.ibm.amoeba.fs.{FileSystem, Inode, InodePointer, InodeTable}

import scala.concurrent.{ExecutionContext, Future}

class SimpleInodeTable(
                      val fs: FileSystem,
                      val inodeAllocator: ObjectAllocator,
                      val root: RootManager
                      ) extends InodeTable {

  implicit val ec: ExecutionContext = fs.executionContext

  protected val rnd = new java.util.Random

  protected val table = new TieredKeyValueList(fs.client, root)

  protected var nextInodeNumber: Long = rnd.nextLong()

  protected def allocateInode(): Long = synchronized {
    val t = nextInodeNumber
    nextInodeNumber += 1
    t
  }

  protected def selectNewInodeAllocationPosition(): Unit = synchronized {
    nextInodeNumber = rnd.nextLong()

    while (nextInodeNumber == InodeTable.NullInode)
      nextInodeNumber = rnd.nextLong()
  }

  /** Future completes when the transaction is ready for commit */
  override def prepareInodeAllocation(inode: Inode)(implicit tx: Transaction): Future[InodePointer] = {

    // Jump to new location if the transaction fails for any reason
    tx.result.failed.foreach( _ => selectNewInodeAllocationPosition() )

    val inodeNumber = allocateInode()
    val updatedInode = inode.update(inodeNumber=Some(inodeNumber))
    val key = Key(inodeNumber)
    val requirements = DoesNotExist(key) :: Nil

    Future.failed(new Exception("Not done yet"))
/*
    for {
      node <- table.fetchMutableNode(inodeNumber)
      ptr <- inodeAllocator.allocateDataObject(node.kvos.pointer, node.kvos.revision, updatedInode.toDataBuffer)
      _=tx.note(s"Allocating new inode $inodeNumber with inode object uuid ${ptr.uuid} and file type: ${inode.fileType}")
      iptr = InodePointer(inode.fileType, inodeNumber, ptr)
      _<-node.prepreUpdateTransaction(List((key, iptr.toArray)), Nil, requirements)
    } yield {
      iptr
    }

 */
  }
  /** Removes the Inode from the table. This method does NOT decrement the reference count on the Inode object. */
  override def delete(pointer: InodePointer)(implicit tx: Transaction): Future[Unit] = {
    table.delete(Key(pointer.number))
  }

  override def lookup(inodeNumber: Long): Future[Option[InodePointer]] = {
    for {
      ovs <- table.get(Key(inodeNumber))
    } yield {
      ovs match {
        case None => None
        case Some(vs) => Some(InodePointer(vs.value.bytes))
      }
    }
  }
}
