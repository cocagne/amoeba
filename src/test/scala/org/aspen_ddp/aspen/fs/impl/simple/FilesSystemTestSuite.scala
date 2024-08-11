package org.aspen_ddp.aspen.fs.impl.simple

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevisionGuard}
import org.aspen_ddp.aspen.fs.FileSystem
import org.aspen_ddp.aspen.fs.impl.simple.SimpleFileSystem

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
