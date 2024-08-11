package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.util.BackgroundTask
import org.aspen_ddp.aspen.server.crl.CrashRecoveryLogFactory
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}

import java.nio.file.Path
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

class SingleThreadedStoreManager(rootDir: Path,
                                 ec: ExecutionContext,objectCacheFactory: () => ObjectCache,
                                 net: Messenger,
                                 backgroundTasks: BackgroundTask,
                                 crlFactory: CrashRecoveryLogFactory,
                                 finalizerFactory: TransactionFinalizer.Factory,
                                 txDriverFactory: TransactionDriver.Factory,
                                 heartbeatPeriod: Duration) extends StoreManager(rootDir, ec, objectCacheFactory,
  net, backgroundTasks, crlFactory, finalizerFactory, txDriverFactory,  heartbeatPeriod){

  private val managerThread = new Thread {
    override def run(): Unit = {
      while (!shutdownCalled) {
        handleEvents()
        awaitEvent()
      }
    }
  }

  managerThread.start()
}
