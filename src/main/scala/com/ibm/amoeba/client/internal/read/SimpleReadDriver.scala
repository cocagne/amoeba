package com.ibm.amoeba.client.internal.read

import java.util.UUID

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.common.objects.ObjectPointer
import com.ibm.amoeba.common.util.BackgroundTask

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object SimpleReadDriver {
  class Factory(
                 val initialDelay: Duration,
                 val maxDelay: Duration){
    def apply(
               client: AmoebaClient,
               objectPointer: ObjectPointer,
               readUUID:UUID,
               disableOpportunisticRebuild: Boolean): ReadDriver = {
      new SimpleReadDriver(initialDelay, maxDelay, client,
        objectPointer, readUUID, disableOpportunisticRebuild)
    }
  }
}

/** This class provides a *very* simple exponential backoff retry mechanism for reads in that completes when either the object
  *  is successfully read or a fatal error is encountered.
  *
  */
class SimpleReadDriver(
                        val initialDelay: Duration,
                        val maxDelay: Duration,
                        client: AmoebaClient,
                        objectPointer: ObjectPointer,
                        readUUID:UUID,
                        disableOpportunisticRebuild: Boolean) extends BaseReadDriver( client,
  objectPointer, readUUID, disableOpportunisticRebuild) {

  implicit protected val ec: ExecutionContext = client.clientContext

  private[this] var task: Option[BackgroundTask.ScheduledTask] = None

  override def onComplete(): Unit = synchronized {
    logger.trace(s"READ UUID $readUUID: COMPLETE. Cancelling future retransmits")
    task.foreach(_.cancel())
  }

  override def begin(): Unit = synchronized {
    task = Some(client.backgroundTasks.retryWithExponentialBackoff(tryNow=false, initialDelay=initialDelay, maxDelay=maxDelay) {
      super.begin()
      false
    })
    super.begin()
  }

  override def shutdown(): Unit = task.foreach( t => t.cancel() )
}
