package com.ibm.amoeba.client.internal

import com.ibm.amoeba.common.objects.ObjectPointer
import com.ibm.amoeba.common.transaction.PreTransactionOpportunisticRebuild
import com.ibm.amoeba.server.store.ObjectState

import scala.concurrent.duration.Duration

trait OpportunisticRebuildManager {

  /** Informs the OpportunisticRebuildManager that the stores with pool indicies in repairNeeded are in
    * need of repairs
    */
  def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit

  def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, PreTransactionOpportunisticRebuild]

  /** Defines how long the reads should await tardy store reads for updating the rebuild manager. Responses
    * received after this duration may be dropped
    */
  def slowReadReplyDuration: Duration
}

object OpportunisticRebuildManager {

  object None extends OpportunisticRebuildManager {
    def markRepairNeeded(os: ObjectState, repairNeeded: Set[Byte]): Unit = ()

    def getPreTransactionOpportunisticRebuild(pointer: ObjectPointer): Map[Byte, PreTransactionOpportunisticRebuild] = Map()

    def slowReadReplyDuration: Duration = Duration.Zero
  }

}
