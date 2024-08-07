package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.IntegrationTestSuite
import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.common.objects.{Key, ObjectRevisionGuard}
import com.ibm.amoeba.fs.FileSystem

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

class FilesSystemTestSuite  extends IntegrationTestSuite {
  var fs: FileSystem = _

  //override def subFixtureSetup(): Unit = {
  def bootstrap(): Future[FileSystem] = {
    for {
      kvos <- client.read(nucleus)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new SinglePoolObjectAllocator(client, rootPool.get, nucleus.ida, None )
      fs <- SimpleFileSystem.bootstrap(client,
        ObjectRevisionGuard(nucleus, kvos.revision),
        allocator,
        kvos.pointer,
        Key(100))
    } yield {
      fs
    }
  }

  override def subFixtureTeardown(): Unit = {
    if (fs != null)
      fs.shutdown()
    fs = null
  }
}
