package com.ibm.amoeba.client.internal.read

import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState, ReadError}
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.network.{OpportunisticRebuild, Read, ReadResponse}
import com.ibm.amoeba.common.objects.{DataObjectPointer, FullObject, KeyValueObjectPointer, MetadataOnly, ObjectPointer, ReadType}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.util.{BackgroundTask, BackgroundTaskPool}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

abstract class BaseReadDriver(
                      val client: AmoebaClient,
                      val objectPointer: ObjectPointer,
                      val readUUID:UUID,
                      val disableOpportunisticRebuild: Boolean = false
                    ) extends ReadDriver with Logging {

  implicit protected val ec: ExecutionContext

  logger.info(s"Read UUID $readUUID: Beginning read of ${objectPointer.objectType} object ${objectPointer.id}")

  // TODO: Support multiple read types
  private val readType: ReadType = FullObject()

  // Detect invalid combinations
  val objectReader: ObjectReader = (objectPointer, readType) match {
    case (p: KeyValueObjectPointer, _:MetadataOnly) => new KeyValueObjectReader(true, p, readUUID)
    case (p: KeyValueObjectPointer, _) => new KeyValueObjectReader(false, p, readUUID)

    case (p: DataObjectPointer,     _:MetadataOnly) => new DataObjectReader(true, p, readUUID)
    case (p: DataObjectPointer,     _:FullObject) => new DataObjectReader(false, p, readUUID)

    case _ => throw new AssertionError("Invalid read combination")
  }

  protected var retryCount = 0

  private var rebuildsSent: Set[StoreId] = Set()

  protected val promise: Promise[ObjectState] = Promise()

  def readResult: Future[ObjectState] = promise.future

  def begin(): Unit = sendReadRequests()

  def shutdown(): Unit = {}

  protected def onComplete(): Unit = ()

  /** Sends a Read request to the specified store. */
  protected def sendReadRequestNoLogMessage(dataStoreId: StoreId): Unit = {
    client.messenger.sendClientRequest(Read(dataStoreId, client.clientId, readUUID, objectPointer, readType))
  }

  /** Sends a Read request to the specified store. */
  protected def sendReadRequest(dataStoreId: StoreId): Unit = {
    logger.trace(s"Read UUID $readUUID: Sending read request for object ${objectPointer.id} to store $dataStoreId")
    client.messenger.sendClientRequest(Read(dataStoreId, client.clientId, readUUID, objectPointer, readType))
  }

  /** Sends a Read request to all stores that have not already responded. May be called outside a synchronized block */
  protected def sendReadRequests(): Unit = {
    //logger.trace(s"Read UUID $readUUID: sending read requests to all stores for object ${objectPointer.id}. ${objectPointer.storePointers.map(sp => StoreId(objectPointer.poolId, sp.poolIndex)).toList}")
    synchronized {
      retryCount += 1
      if (retryCount > 3)
        logger.debug(s"RESENDING READ REQUESTS for Read UUID $readUUID")
    }
    //objectPointer.storePointers.foreach(sp => sendReadRequestNoLogMessage(StoreId(objectPointer.poolId, sp.poolIndex)))
    objectPointer.storePointers.foreach(sp => sendReadRequest(StoreId(objectPointer.poolId, sp.poolIndex)))
  }

  protected def sendOpportunisticRebuild(storeId: StoreId, os: ObjectState): Unit = {
    if (!rebuildsSent.contains(storeId)) {
      logger.info(s"Read UUID $readUUID: Sending Opprotunistic Rebuild to store $storeId for objerct ${objectPointer.id}")
      rebuildsSent += storeId
      client.messenger.sendClientRequest(OpportunisticRebuild(storeId, client.clientId, objectPointer, os.revision,
        os.refcount, os.timestamp, os.getRebuildDataForStore(storeId).get))
    }
  }

  def receiveReadResponse(response:ReadResponse): Boolean = synchronized {
    logger.trace(s"Read UUID $readUUID: Received read response from store ${response.fromStore}")

    val hasLocksForKnownCommittedTransactions = response.result match {
      case Left(_) => false
      case Right(cs) => ! cs.lockedWriteTransactions.forall { txuuid =>
        client.txStatusCache.getTransactionResolution(txuuid) match {
          case None => true
          case Some((result, _)) => !result
        }
      }
    }

    if (hasLocksForKnownCommittedTransactions) {
      // We know for certain that the response from this store has out-of-date information. Transactions _should_
      // resolve quickly so we'll immediately reread from the store
      sendReadRequest(response.fromStore)
    }
    else {
      objectReader.receiveReadResponse(response).foreach { e =>
        if (!promise.isCompleted) {
          val result = e match {
            case Left(err) => Left(err)
            case Right(os) =>

              // Ensure any commit transactions will use timestamps after all read objects last update time
              os match {
                case dos: DataObjectState => HLCTimestamp.update(dos.timestamp)
                case kvos: KeyValueObjectState => HLCTimestamp.update(kvos.lastUpdateTimestamp)
                case mos: MetadataObjectState => HLCTimestamp.update(mos.timestamp)
              }

              client.objectCache.put(objectPointer, os)

              Right(os)
          }

          result match {
            case Left(err) =>
              logger.info(s"Read UUID $readUUID: Failed to read object ${objectPointer.id}. Reason: $err")
              promise.failure(err)
              onComplete()

            case Right(obj) =>
              obj match {
                case dos: DataObjectState => logger.info(s"Read UUID $readUUID: Successfully read DataObject ${objectPointer.id} Rev ${dos.revision} Ref ${dos.refcount} Size ${dos.data.size} Hash ${dos.data.hashString}")
                case kvos: KeyValueObjectState => logger.info(s"Read UUID $readUUID: Successfully read KeyValueObject ${objectPointer.id} Rev ${kvos.revision} Ref ${kvos.refcount} Num Entries ${kvos.contents.size}")
                case mos: MetadataObjectState => logger.info(s"Read UUID $readUUID: Successfully read MetadataObject ${objectPointer.id} Rev ${mos.revision} Ref ${mos.refcount}")
              }
              onComplete()
              promise.success(obj)
          }
        }

        if (promise.isCompleted) {
          e match {
            case Right(os) =>

              val repairNeeded: Set[Byte] = if (disableOpportunisticRebuild)
                Set()
              else
                objectReader.rereadCandidates.keys.map(_.poolIndex).toSet

              client.opportunisticRebuildManager.markRepairNeeded(os, repairNeeded)

            case _ =>
          }
        }

        e match {
          case Right(os) =>
            if (objectReader.rereadCandidates.contains(response.fromStore))
              sendOpportunisticRebuild(response.fromStore, os)
          case _ =>
        }
      }
    }
    objectReader.receivedResponsesFromAllStores
  }


}

object BaseReadDriver {

  def noErrorRecoveryReadDriver(
    client: AmoebaClient,
    objectPointer: ObjectPointer,
    readUUID:UUID,
    disableOpportunisticRebuild: Boolean): ReadDriver = {

    new BaseReadDriver(client, objectPointer, readUUID) {

      implicit protected val ec: ExecutionContext = client.clientContext

      var hung = false

      val bgTasks = new BackgroundTaskPool

      val hangCheckTask: BackgroundTask.ScheduledTask = bgTasks.schedule(Duration(2, SECONDS)) {
        val test = client.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST")
        //println(s"**** HUNG READ: $test")

        objectReader.debugLogStatus(s"*** HUNG READ in test $test", println)
        // KeyValueObjectCodec.isRestorable(objectPointer.ida,
        //   storeStates.valuesIterator.map(ss => ss.asInstanceOf[KeyValueObjectStoreState].kvoss).toList)

        synchronized(hung = true)
      }

      readResult.foreach { _ =>

        hangCheckTask.cancel()
        bgTasks.shutdown(Duration(0, SECONDS))
        synchronized {
          if (hung) {
            val test = client.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST")
            println(s"**** HUNG READ EVENTUALLY COMPLETED! : $test")
          }
        }
      }(ec)
    }
  }
}
