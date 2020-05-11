package com.ibm.amoeba.fs

import com.ibm.amoeba.client.tkvl.{Root, SinglePoolNodeAllocator}
import com.ibm.amoeba.client.{FatalReadError, StopRetrying, Transaction}
import com.ibm.amoeba.common.objects.{LexicalKeyOrdering, ObjectRevision}
import com.ibm.amoeba.fs.error.{DirectoryEntryDoesNotExist, DirectoryEntryExists, DirectoryNotEmpty, InvalidInode}
import com.ibm.amoeba.fs.impl.simple.CreateFileTask
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}

trait Directory extends BaseFile with Logging {
  val pointer: DirectoryPointer
  val fs: FileSystem

  private implicit val ec: ExecutionContext = fs.executionContext

  //def inode: DirectoryInode

  def getInode(): Future[(DirectoryInode, ObjectRevision)] = {
    fs.readInode(pointer).map(t => (t._1.asInstanceOf[DirectoryInode], t._3))
  }

  def lookup(name: String): Future[Option[InodePointer]] = name match {
    case "." => Future.successful(Some(pointer))
    case ".." => getInode().map( _._1.oparent )
    case _ => getEntry(name)
  }

  def isEmpty(): Future[Boolean]

  def getContents(): Future[List[DirectoryEntry]]

  def getEntry(name: String): Future[Option[InodePointer]]

  def prepareInsert(name: String, pointer: InodePointer, incref: Boolean=true)(implicit tx: Transaction): Future[Unit]

  def prepareDelete(name: String, decref: Boolean=true)(implicit tx: Transaction): Future[Unit]

  def prepareRename(oldName: String, newName: String)(implicit tx: Transaction): Future[Unit]

  def prepareHardLink(name: String, file: BaseFile)(implicit tx: Transaction): Future[Unit]

  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits
    */
  def prepareForDirectoryDeletion()(implicit tx: Transaction): Future[Unit]

  private def retryUntilSuccessfulOr[T](prepare: Transaction => Future[T])
                                       (checkForErrors: => Future[Unit]): Future[T] = {
    def onFail(err: Throwable): Future[Unit] = {

      err match {
        case e: InvalidInode => throw StopRetrying(e)
        case e: FatalReadError => throw StopRetrying(e)
        case e: DirectoryNotEmpty => throw StopRetrying(e)
        case _ =>
          logger.info(s"retryUntilSuccessOr error $err")
          refresh().recover {
            case e: InvalidInode => throw StopRetrying(e)
            case other => throw other
          }.flatMap(_ => checkForErrors)
      }
    }
    fs.client.transactUntilSuccessfulWithRecovery(onFail) { tx =>
      prepare(tx)
    }
  }

  private def retryCreationOr[T](prepare: Transaction => Future[Future[T]])
                                (checkForErrors: => Future[Unit]): Future[T] = {
    val p = Promise[T]()

    def onFail(err: Throwable): Future[Unit] = {
      err match {
        case e: InvalidInode => throw StopRetrying(e)
        case e: FatalReadError => throw StopRetrying(e)
        case _ => refresh().recover {
          case e: InvalidInode => throw StopRetrying(e)
          case other => throw other
        }.flatMap(_ => checkForErrors)
      }
    }

    val fcreate = fs.client.transactUntilSuccessfulWithRecovery(onFail) { tx =>
      prepare(tx)
    }

    fcreate.foreach { ft =>
      ft.foreach(p.success)
      ft.failed.foreach(p.failure)
    }

    fcreate.failed.foreach(p.failure)

    p.future
  }

  def insert(name: String, fpointer: InodePointer, incref: Boolean=true): Future[Unit] = {
    retryUntilSuccessfulOr { implicit tx =>
      prepareInsert(name, fpointer, incref)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }


  def delete(name: String, decref: Boolean=true): Future[Unit] = {
    retryUntilSuccessfulOr { implicit tx =>
      prepareDelete(name, decref)
    }{
      getEntry(name).map {
        case None =>  throw StopRetrying(DirectoryEntryDoesNotExist(pointer, name))
        case Some(_) =>
      }
    }
  }

  def rename(oldName: String, newName: String): Future[Unit] = {
    retryUntilSuccessfulOr { implicit tx =>
      prepareRename(oldName, newName)
    }{
      Future.sequence(getEntry(oldName) :: getEntry(newName) :: Nil).map { lst =>
        if (lst.head.isEmpty)
          throw StopRetrying(DirectoryEntryDoesNotExist(pointer, oldName))

        if (lst.tail.head.nonEmpty)
          throw StopRetrying(DirectoryEntryExists(pointer, newName))
      }
    }
  }

  def hardLink(name: String, f: BaseFile): Future[Unit] = {
    retryUntilSuccessfulOr { implicit tx =>
      prepareHardLink(name, f)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createDirectory(name: String, mode: Int, uid: Int, gid: Int): Future[DirectoryPointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateDirectory(name, mode, uid, gid)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createFile(name: String, mode: Int, uid: Int, gid: Int): Future[FilePointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateFile(name, mode, uid, gid)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String): Future[SymlinkPointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateSymlink(name, mode, uid, gid, link)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createUnixSocket(name: String, mode: Int, uid: Int, gid: Int): Future[UnixSocketPointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateUnixSocket(name, mode, uid, gid)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createFIFO(name: String, mode: Int, uid: Int, gid: Int): Future[FIFOPointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateFIFO(name, mode, uid, gid)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): Future[CharacterDevicePointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateCharacterDevice(name, mode, uid, gid, rdev)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def createBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int): Future[BlockDevicePointer] = {
    retryCreationOr { implicit tx =>
      prepareCreateBlockDevice(name, mode, uid, gid, rdev)
    }{
      getEntry(name).map {
        case None =>
        case Some(_) => throw StopRetrying(DirectoryEntryExists(pointer, name))
      }
    }
  }

  def prepareSetParentDirectory(parent: Directory)(implicit tx: Transaction, ec: ExecutionContext): Unit = {
    val updatedInode = inode.asInstanceOf[DirectoryInode].setParentDirectory(Some(parent.pointer))

    tx.overwrite(pointer.pointer, revision, updatedInode.toDataBuffer)

    tx.result.foreach { _ =>
      setCachedInode(updatedInode, tx.revision)
    }
  }

  def prepareCreateDirectory(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction): Future[Future[DirectoryPointer]] = {
    val root = Root(0, LexicalKeyOrdering, None, new SinglePoolNodeAllocator(fs.client, pointer.pointer.poolId))
    val newInode = DirectoryInode.init(mode, uid, gid, Some(pointer), None, root)
    val raw =  CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[DirectoryPointer]
    }
    raw.map(_ => fdp)
  }

  def prepareCreateFile(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction): Future[Future[FilePointer]] = {
    val newInode = FileInode.init(mode, uid, gid)

    val raw = CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[FilePointer]
    }
    raw.map(_ => fdp)
  }

  def prepareCreateSymlink(name: String, mode: Int, uid: Int, gid: Int, link: String)(implicit tx: Transaction): Future[Future[SymlinkPointer]] = {
    val newInode = SymlinkInode.init(mode, uid, gid, link)

    val raw = CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[SymlinkPointer]
    }
    raw.map(_ => fdp)
  }

  def prepareCreateUnixSocket(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction): Future[Future[UnixSocketPointer]] = {
    val newInode = UnixSocketInode.init(mode, uid, gid)

    val raw = CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[UnixSocketPointer]
    }
    raw.map(_ => fdp)
  }

  def prepareCreateFIFO(name: String, mode: Int, uid: Int, gid: Int)(implicit tx: Transaction): Future[Future[FIFOPointer]] = {
    val newInode = FIFOInode.init(mode, uid, gid)

    val raw = CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[FIFOPointer]
    }
    raw.map(_ => fdp)
  }

  def prepareCreateCharacterDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit tx: Transaction): Future[Future[CharacterDevicePointer]] = {
    val newInode = CharacterDeviceInode.init(mode, uid, gid, rdev)

    val raw = CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[CharacterDevicePointer]
    }
    raw.map(_ => fdp)
  }

  def prepareCreateBlockDevice(name: String, mode: Int, uid: Int, gid: Int, rdev: Int)(implicit tx: Transaction): Future[Future[BlockDevicePointer]] = {
    val newInode = BlockDeviceInode.init(mode, uid, gid, rdev)

    val raw = CreateFileTask.prepareTask(fs, pointer, name, newInode)
    val fdp = for {
      fof <- raw
      of <- fof
    } yield {
      of.get.asInstanceOf[BlockDevicePointer]
    }
    raw.map(_ => fdp)
  }
}
