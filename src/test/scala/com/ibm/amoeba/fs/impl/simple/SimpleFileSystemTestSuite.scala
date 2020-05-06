package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.common.objects.ObjectRevisionGuard
import com.ibm.amoeba.fs.FileType

class SimpleFileSystemTestSuite extends FilesSystemTestSuite {
  /*test("Load root directory pointer") {
    bootFS()
    for {
      (rootInode, _) <- fs.readInode(1)
    } yield {
      rootInode.fileType should be (FileType.Directory)
    }
  }*/
  test("Load root directory pointer") {
    for {
      /*kvos <- client.read(nucleus)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new SinglePoolObjectAllocator(client, rootPool, nucleus.ida, None )
      fs <- SimpleFileSystem.bootstrap(client, ObjectRevisionGuard(nucleus, kvos.revision), allocator)
       */
      fs <- bootFS()
      (rootInode, _) <- fs.readInode(1)
    } yield {
      rootInode.fileType should be (FileType.Directory)
    }
  }
}
