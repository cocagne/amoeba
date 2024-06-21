package com.ibm.amoeba.server

import com.ibm.amoeba.common.util.BackgroundTask
import com.ibm.amoeba.server.crl.CrashRecoveryLogFactory
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.Backend
import com.ibm.amoeba.server.store.cache.ObjectCache
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer}

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
