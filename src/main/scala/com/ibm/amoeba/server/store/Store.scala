package com.ibm.amoeba.server.store

import com.github.blemale.scaffeine.Scaffeine
import com.ibm.amoeba.common.network.{TxAcceptResponse, TxCommitted, TxFinalized, TxMessage, TxPrepare, TxPrepareResponse, TxResolved}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.CrashRecoveryLog
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.Backend
import com.ibm.amoeba.server.store.cache.ObjectCache
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}

import scala.concurrent.duration._

class Store(val backend: Backend,
            val objectCache: ObjectCache,
            val net: Messenger,
            val crl: CrashRecoveryLog,
            val txStatusCache: TransactionStatusCache,
            val finalizerFactory: TransactionFinalizer.Factory,
            val txDriverFactory: TransactionDriver.Factory) {

  val storeId: StoreId = backend.storeId
  val frontend = new Frontend(backend.storeId, backend, objectCache, net, crl, txStatusCache)

  private var transactionDrivers: Map[TransactionId, TransactionDriver] = Map()

  private[this] val prepareResponseCache = Scaffeine().expireAfterWrite(10.seconds)
    .maximumSize(1000)
    .build[TransactionId, List[TxPrepareResponse]]()

  def receiveTransactionMessage(msg: TxMessage): Unit = {

    msg match {
      case m: TxPrepare =>
        txStatusCache.getStatus(m.txd.transactionId).foreach { entry =>
          val (r, committed: Boolean) = entry.status match {
            case TransactionStatus.Unresolved => (None, false)
            case TransactionStatus.Aborted =>
              (Some(TxResolved(msg.to, msg.from, m.txd.transactionId, committed = false)), false)
            case TransactionStatus.Committed =>
              (Some(TxResolved(msg.to, msg.from, m.txd.transactionId, committed = true)), true)
          }

          r.foreach(resolved => net.sendTransactionMessage(resolved))

          if (entry.finalized)
            net.sendTransactionMessage(TxFinalized(msg.from, msg.to, m.txd.transactionId, committed))
        }

        if (m.txd.primaryObject.poolId == storeId.poolId && m.txd.designatedLeaderUID == storeId.poolIndex) {
          if (!transactionDrivers.contains(m.txd.transactionId)) {
            val driver = txDriverFactory.create(storeId, net, m.txd, finalizerFactory)

            transactionDrivers += m.txd.transactionId -> driver

            driver.receiveTxPrepare(m)
            prepareResponseCache.getIfPresent(m.txd.transactionId).foreach { lst =>
              lst.foreach(driver.receiveTxPrepareResponse(_, txStatusCache))
              prepareResponseCache.invalidate(m.txd.transactionId)
            }
          }
        }

      case m: TxPrepareResponse =>
        // cache if driver doesn't exist deliver otherwise
        transactionDrivers.get(m.transactionId) match {
          case Some(driver) => driver.receiveTxPrepareResponse(m, txStatusCache)
          case None =>
            val lst = prepareResponseCache.getIfPresent(m.transactionId).getOrElse(Nil)
            prepareResponseCache.put(m.transactionId, m :: lst)
        }

      case m: TxAcceptResponse => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxAcceptResponse(m)
      }

      case m: TxResolved => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxResolved(m)
      }

      case m: TxCommitted => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxCommitted(m)
      }

      case m: TxFinalized => transactionDrivers.get(m.transactionId).foreach { driver =>
        driver.receiveTxFinalized(m)
      }

      case _ =>
    }

    frontend.receiveTransactionMessage(msg)
  }
}
