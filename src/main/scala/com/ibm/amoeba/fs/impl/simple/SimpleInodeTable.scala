package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.client.{ObjectAllocator, Transaction}
import com.ibm.amoeba.client.tkvl.RootManager
import com.ibm.amoeba.common.objects.Key
import com.ibm.amoeba.common.transaction.KeyValueUpdate
import com.ibm.amoeba.fs.{FileSystem, Inode, InodePointer, InodeTable}

import scala.concurrent.{ExecutionContext, Future}

class SimpleInodeTable(
                      val fs: FileSystem,
                      val inodeAllocator: ObjectAllocator,
                      val root: RootManager
                      ) extends InodeTable {

  implicit val ec: ExecutionContext = fs.executionContext

  protected val rnd = new java.util.Random

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
  override def prepareInodeAllocation(inode: Inode)(implicit tx: Transaction): Future[InodePointer] = ???
  /** Removes the Inode from the table. This method does NOT decrement the reference count on the Inode object. */
  override def delete(pointer: InodePointer): Future[Unit] = ???

  override def lookup(inodeNumber: Long): Future[Option[InodePointer]] = ???
}
