package com.ibm.amoeba.fs

import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator, ObjectAllocatorId}
import com.ibm.amoeba.common.objects.ObjectRevision

import scala.concurrent.{ExecutionContext, Future}

trait FileSystem {
  val uuid: UUID

  def readInode(iptr: InodePointer): Future[(Inode, ObjectRevision)] = {
    implicit val ec: ExecutionContext = executionContext
    client.read(iptr.pointer).map { dos =>
      (Inode(client, dos.data), dos.revision)
    }
  }

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
