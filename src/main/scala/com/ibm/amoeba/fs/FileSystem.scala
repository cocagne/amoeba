package com.ibm.amoeba.fs

import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, CorruptedObject, InvalidObject, ObjectAllocator, ObjectAllocatorId, RetryStrategy}
import com.ibm.amoeba.common.objects.ObjectRevision
import com.ibm.amoeba.compute.TaskExecutor
import com.ibm.amoeba.fs.error.InvalidInode
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait FileSystem extends Logging {
  val uuid: UUID

  def readInode(inodeNumber: Long)(implicit ec: ExecutionContext): Future[(Inode, InodePointer, ObjectRevision)] = {
    inodeTable.lookup(inodeNumber).flatMap {
      case None => Future.failed(InvalidInode(inodeNumber))
      case Some(iptr) => readInode(iptr)
    }
  }
  def readInode(iptr: InodePointer): Future[(Inode, InodePointer, ObjectRevision)] = {
    implicit val ec: ExecutionContext = executionContext
    //client.read(iptr.pointer).map { dos =>
    //  (Inode(client, dos.data), dos.revision)
    //}
    val pload = Promise[(Inode, InodePointer, ObjectRevision)]()

    client.read(iptr.pointer) onComplete {
      case Success(dos) =>
        pload.success((Inode(client, dos.data), iptr, dos.revision))

      case Failure(_: InvalidObject) =>
        // Probably deleted
        pload.failure(InvalidInode(iptr.number))

      case Failure(e: CorruptedObject) =>
        // TODO: If pointer fails to read, we'll need to read the inode table (could be stale pointer). If new pointer
        //       for that inode exists, we'll need to use a callback function to update the stale pointer
        pload.failure(e)
        logger.error(s"Corrupted Inode: $iptr")

      case Failure(cause) =>
        pload.failure(cause)
        logger.error(s"Unexpected error encountered during load of inode $iptr, $cause")
    }

    pload.future
  }

  def shutdown(): Unit

  private[fs] def retryStrategy: RetryStrategy
  private[fs] def taskExecutor: TaskExecutor
  private[fs] def defaultInodeAllocater: ObjectAllocator
  private[fs] def client: AmoebaClient
  private[fs] def executionContext: ExecutionContext
  private[fs] def getObjectAllocator(id: ObjectAllocatorId): Future[ObjectAllocator]
  private[fs] def inodeTable: InodeTable
}

object FileSystem {
  private[this] var loadedFileSystems = Map[UUID, FileSystem]()

  def register(fs: FileSystem): Unit = synchronized {
    loadedFileSystems += (fs.uuid -> fs)
  }

  def getRegisteredFileSystem(uuid: UUID): Option[FileSystem] = synchronized {
    loadedFileSystems.get(uuid)
  }
}
