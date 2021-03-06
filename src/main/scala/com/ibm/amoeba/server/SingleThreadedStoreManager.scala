package com.ibm.amoeba.server

import com.ibm.amoeba.common.util.BackgroundTask
import com.ibm.amoeba.server.crl.CrashRecoveryLogFactory
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.Backend
import com.ibm.amoeba.server.store.cache.ObjectCache
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer}

import scala.concurrent.duration.Duration

class SingleThreadedStoreManager(objectCacheFactory: () => ObjectCache,
                                 net: Messenger,
                                 backgroundTasks: BackgroundTask,
                                 crlFactory: CrashRecoveryLogFactory,
                                 finalizerFactory: TransactionFinalizer.Factory,
                                 txDriverFactory: TransactionDriver.Factory,
                                 initialBackends: List[Backend],
                                 heartbeatPeriod: Duration) extends StoreManager(objectCacheFactory,
  net, backgroundTasks, crlFactory, finalizerFactory, txDriverFactory,  heartbeatPeriod, initialBackends){

  private val managerThread = new Thread {
    override def run(): Unit = {
      while (!shutdownCalled) {
        handleEvents()
        awaitEvent()
      }
    }
  }

  initialBackends.foreach(loadStore)

  managerThread.start()

}
