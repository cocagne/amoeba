package com.ibm.amoeba.server

import com.ibm.amoeba.server.crl.CrashRecoveryLogFactory
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.{TransactionDriver, TransactionFinalizer}
import com.ibm.amoeba.server.store.backend.Backend
import com.ibm.amoeba.server.store.cache.ObjectCache

class SingleThreadedStoreManager(objectCache: ObjectCache,
                                 net: Messenger,
                                 crlFactory: CrashRecoveryLogFactory,
                                 finalizerFactory: TransactionFinalizer.Factory,
                                 txDriverFactory: TransactionDriver.Factory,
                                 initialBackends: List[Backend]) extends StoreManager(objectCache,
  net, crlFactory, finalizerFactory, txDriverFactory, initialBackends){

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
