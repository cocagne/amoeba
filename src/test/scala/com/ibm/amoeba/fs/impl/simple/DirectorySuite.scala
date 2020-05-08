package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.client.Transaction
import com.ibm.amoeba.common.objects.{ObjectPointer, ObjectRevision}
import com.ibm.amoeba.fs.error.DirectoryNotEmpty
import com.ibm.amoeba.fs.{Directory, DirectoryPointer, FileMode, Timespec}

import scala.concurrent.Future

class DirectorySuite extends FilesSystemTestSuite {
  def cdir(dir: Directory, name: String, mode: Int, uid: Int, gid: Int): Future[DirectoryPointer] = {
    implicit val tx: Transaction = dir.fs.client.newTransaction()
    val fprep = dir.prepareCreateDirectory(name, mode, uid, gid)
    fprep.foreach(_ => tx.commit())
    fprep.flatMap(fresult => fresult)
  }

  test("Amoeba Bootstrap") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      (rootInode, _) <- rootDir.getInode()
    } yield {
      rootInode.uid should be (0)
    }
  }

  test("Create Directory") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      (newInode, _) <- newDir.getInode()
      newContent <- rootDir.getContents()
    } yield {
      initialContent.length should be (0)
      newInode.uid should be (1)
      newInode.gid should be (2)
      newContent.length should be (1)
      newContent.head.name should be ("foo")
    }
  }

  test("Change Directory UID") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      newDirPointer <- cdir(rootDir,"foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      origUID = newDir.uid
      _ <- newDir.setUID(5)
      newDir2 <- fs.loadDirectory(newDirPointer)
    } yield {
      origUID should be (1)
      newDir.uid should be (5)
      newDir2.uid should be (5)
    }
  }

  test("Change Directory UID with recovery from revision mismatch") {
    def vbump(ptr: ObjectPointer, revision: ObjectRevision): Future[Unit] = {
      implicit val tx: Transaction = client.newTransaction()
      tx.bumpVersion(ptr, revision)
      tx.commit().map(_=>())
    }

    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()

      newDirPointer <- cdir(rootDir,"foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      (_, revision) <- newDir.getInode()
      origUID = newDir.uid

      _ <- vbump(newDir.pointer.pointer, revision)

      _ <- newDir.setUID(5)

      newDir2 <- fs.loadDirectory(newDirPointer)
    } yield {
      origUID should be (1)
      newDir.uid should be (5)
      newDir2.uid should be (5)
    }
  }

  test("Change multiple metadata attributes") {
    val u = 6
    val g = 7
    val m = 1
    val ct = Timespec(1,2)
    val mt = Timespec(3,4)
    val at = Timespec(4,5)
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      _ <- rootDir.getContents()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      fu = newDir.setUID(u)
      fg = newDir.setGID(g)
      fm = newDir.setMode(m)
      fc = newDir.setCtime(ct)
      fx = newDir.setMtime(mt)
      fa = newDir.setAtime(at)
      _ <- Future.sequence(List(fu, fg, fm, fc, fx, fa))
      d <- fs.loadDirectory(newDirPointer)
    } yield {
      d.uid should be (u)
      d.gid should be (g)
      d.mode should be (m | FileMode.S_IFDIR)
      d.ctime should be (ct)
      d.mtime should be (mt)
      d.atime should be (at)
    }
  }

  test("Delete non-empty Directory") {
    for {
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      initialContent <- rootDir.getContents()
      newDirPointer <- cdir(rootDir, "foo", mode=0, uid=1, gid=2)
      newDir <- fs.loadDirectory(newDirPointer)
      _ <- cdir(newDir, "bar", mode=0, uid=1, gid=2)
      dc <- newDir.getContents()
      if dc.length == 1
      _ <- recoverToSucceededIf[DirectoryNotEmpty](rootDir.delete("foo"))
    } yield {
      initialContent.length should be (0)
    }
  }
}
