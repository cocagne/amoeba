package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.client.KeyValueObjectState.ValueState
import com.ibm.amoeba.client.Transaction
import com.ibm.amoeba.client.tkvl.TieredKeyValueList
import com.ibm.amoeba.common.objects.{Key, ObjectRevision, Value}
import com.ibm.amoeba.fs.error.{DirectoryEntryDoesNotExist, DirectoryNotEmpty}
import com.ibm.amoeba.fs.{BaseFile, Directory, DirectoryEntry, DirectoryInode, DirectoryPointer, FileSystem, InodePointer}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Future

class SimpleDirectory(override val pointer: DirectoryPointer,
                      cachedInodeRevision: ObjectRevision,
                      initialInode: DirectoryInode,
                      fs: FileSystem) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with Directory with Logging {

  def tree: TieredKeyValueList = {
    val rootManager = new SimpleDirectoryRootManager(fs.client, pointer.pointer)
    new TieredKeyValueList(fs.client, rootManager)
  }

  override def getContents(): Future[List[DirectoryEntry]] = {

    def scan(sum: List[DirectoryEntry], m: Map[Key, ValueState]): List[DirectoryEntry] = {
      m.foldLeft(sum) { (s, t) =>
        DirectoryEntry(t._1.stringValue, InodePointer(t._2.value.bytes)) :: s
      }
    }

    tree.foldLeft(List[DirectoryEntry]())(scan).map(l => l.reverse)
  }

  override def getEntry(name: String): Future[Option[InodePointer]] = {
    for {
      ovs <- tree.get(Key(name))
    } yield {
      ovs.map(vs => InodePointer(vs.value.bytes))
    }
  }

  override def prepareInsert(name: String,
                             pointer: InodePointer,
                             incref: Boolean)(implicit tx: Transaction): Future[Unit] = {
    val fincref = if (incref) {
      fs.readInode(pointer) map { t =>
        val (finode, _, frevision) = t
        val updatedInode = finode.update(links=Some(inode.links+1))
        tx.overwrite(pointer.pointer, frevision, updatedInode.toArray)
      }
    } else {
      Future.successful(())
    }
    val finsert = tree.set(Key(name), Value(pointer.toArray), Some(Left(true)))

    for {
      _ <- fincref
      _ <- finsert
    } yield ()
  }

  override def prepareDelete(name: String, decref: Boolean)(implicit tx: Transaction): Future[Future[Unit]] = {
    val key = Key(name)

    def onptr(ovs: Option[ValueState]): Future[Unit] = ovs match {
      case None => Future.failed(DirectoryEntryDoesNotExist(pointer, name))
      case Some(vs) =>
        val fptr = InodePointer(vs.value.bytes)
        val fdel = tree.delete(key)
        val fdecref = if (decref) {
          UnlinkFileTask.prepareTask(fs, fptr)
        } else {
          Future.successful(())
        }
        for {
          _ <- fdel
          _ <- fdecref
        } yield {
          tx.result.map(_=>())
        }
    }

    tree.get(key).map(onptr)
  }

  override def prepareRename(oldName: String, newName: String)(implicit tx: Transaction): Future[Unit] = {
    val oldKey = Key(oldName)
    val newKey = Key(newName)
    val tr = tree
    for {
      oa <- tr.getContainingNode(oldKey)
      ob <- tr.getContainingNode(newKey)
      _ <- (oa, ob) match {
        case (Some(a), Some(b)) =>
          if (!a.contains(oldKey)) {
            Future.failed(DirectoryEntryDoesNotExist(pointer, oldKey.stringValue))
          } else {
            if (a.nodeUUID == b.nodeUUID)
              a.rename(oldKey, newKey)
            else {
              Future.sequence(List(a.delete(oldKey), b.set(newKey, a.get(oldKey).get.value)))
            }
          }
        case _ => Future.failed(DirectoryEntryDoesNotExist(pointer, oldName))
      }
    } yield {

    }
  }

  override def prepareHardLink(name: String, file: BaseFile)(implicit tx: Transaction): Future[Unit] = {
    prepareInsert(name, file.pointer)
  }

  /** Ensures the directory is empty and that all resources are cleaned up if the transaction successfully commits
    */
  override def prepareForDirectoryDeletion()(implicit tx: Transaction): Future[Unit] = {
    // TODO: protect against race conditions during directory deletion
    getContents().map { contents =>
      if (contents.nonEmpty)
        throw DirectoryNotEmpty(pointer)
    }
  }


}
