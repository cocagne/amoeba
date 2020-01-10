package com.ibm.amoeba.client.internal.allocation

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, ObjectId, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.transaction.TransactionId
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object SuperSimpleAllocationDriver {

  def factory(retransmitDelay: Duration): AllocationDriver.Factory = {
    new AllocationDriver.Factory {
      def create(client: AmoebaClient,
                 poolId: PoolId,
                 newObjectId: ObjectId,
                 objectSize: Option[Int],
                 objectIDA: IDA,
                 objectData: Map[Byte, DataBuffer], // Map DataStore pool index -> store-specific ObjectData
                 objectType: ObjectType.Value,
                 timestamp: HLCTimestamp,
                 initialRefcount: ObjectRefcount,
                 allocationTransactionId: TransactionId,
                 revisionGuard: AllocationRevisionGuard): BaseAllocationDriver = {
        new SuperSimpleAllocationDriver(retransmitDelay, client, poolId, newObjectId, objectSize, objectIDA, objectData,
          objectType, timestamp, initialRefcount, allocationTransactionId, revisionGuard)
      }
    }
  }

}

class SuperSimpleAllocationDriver(retransmitDelay: Duration,
                                  client: AmoebaClient,
                                  poolId: PoolId,
                                  newObjectId: ObjectId,
                                  objectSize: Option[Int],
                                  objectIDA: IDA,
                                  objectData: Map[Byte, DataBuffer], // Map DataStore pool index -> store-specific ObjectData
                                  objectType: ObjectType.Value,
                                  timestamp: HLCTimestamp,
                                  initialRefcount: ObjectRefcount,
                                  allocationTransactionId: TransactionId,
                                  revisionGuard: AllocationRevisionGuard) extends BaseAllocationDriver(client, poolId,
  newObjectId, objectSize, objectIDA, objectData, objectType, timestamp, initialRefcount, allocationTransactionId,
  revisionGuard) with Logging {

  implicit val ec: ExecutionContext = client.clientContext

  private var retries = 0
  private val retryTask = client.backgroundTasks.schedulePeriodic(period=retransmitDelay) {
    synchronized {
      retries += 1
      if (retries % 3 == 0)
        logger.info(s"***** HUNG Allocation with Transaction $allocationTransactionId")
    }
    sendAllocationMessages()
  }

  futureResult.onComplete { _ => retryTask.cancel() }
}
