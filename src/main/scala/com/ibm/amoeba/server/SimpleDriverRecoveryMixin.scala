package com.ibm.amoeba.server

import com.ibm.amoeba.common.util.BackgroundTask

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}

trait SimpleDriverRecoveryMixin extends StoreManager {

  val checkPeriod = Duration(500, MILLISECONDS)

  private[this] var periodicTask = BackgroundTask.schedulePeriodic(checkPeriod) { addRecoveryEvent() }

  override def shutdown()(implicit ec: ExecutionContext): Future[Unit] = {
    periodicTask.cancel()
    super.shutdown()
  }

  override protected def handleRecoveryEvent(): Unit = {
    val now = System.nanoTime()
    stores.valuesIterator.foreach { store =>
      store.frontend.transactions.valuesIterator.foreach { tx =>
        val delay = Duration(now - tx.lastEventTime, NANOSECONDS)

        if (delay > txDriverFactory.failedDriverDuration)
          store.driveTransaction(tx.txd)
      }
    }
  }
}
