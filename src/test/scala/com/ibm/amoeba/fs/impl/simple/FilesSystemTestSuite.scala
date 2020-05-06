package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.IntegrationTestSuite
import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.common.objects.ObjectRevisionGuard
import com.ibm.amoeba.fs.FileSystem

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class FilesSystemTestSuite  extends IntegrationTestSuite {
  var fs: FileSystem = _

  //override def subFixtureSetup(): Unit = {
  def bootFS(): Future[FileSystem] = {
    for {
      kvos <- client.read(nucleus)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new SinglePoolObjectAllocator(client, rootPool, nucleus.ida, None )
      fs <- SimpleFileSystem.bootstrap(client, ObjectRevisionGuard(nucleus, kvos.revision), allocator)
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
