package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.client.{ObjectAllocator, Transaction}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, KeyOrdering, KeyValueObjectPointer}

import scala.concurrent.Future

trait RootManager {

  /** Returns (numTiers, keyOrdering, rootNode) */
  def getRootNode(): Future[(Int, KeyOrdering, KeyValueListNode)]

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator]

  def getMaxNodeSize(tier: Int): Future[Int]

  def prepareRootUpdate(newTier: Int, newRoot: KeyValueObjectPointer)(implicit tx: Transaction): Future[Unit]

  def getRootRevisionGuard(): Future[AllocationRevisionGuard]

  def encode(): Array[Byte]

  def typeId: RootManagerTypeId
}
