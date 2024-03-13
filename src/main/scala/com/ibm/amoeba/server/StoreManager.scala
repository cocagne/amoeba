package com.ibm.amoeba.server

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.ibm.amoeba.common.network._
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionStatus
import com.ibm.amoeba.common.util.BackgroundTask
import com.ibm.amoeba.server.crl.{CrashRecoveryLogFactory, SaveCompletion, SaveCompletionHandler}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.{Backend, Completion, CompletionHandler}
import com.ibm.amoeba.server.store.cache.ObjectCache
import com.ibm.amoeba.server.store.{Frontend, Store}
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer, TransactionStatusCache}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

object StoreManager {
  sealed abstract class Event

  case class IOCompletion(op: Completion) extends Event
  case class CRLCompletion(op: SaveCompletion) extends Event
  case class TransactionMessage(msg: TxMessage) extends Event
  case class ClientReq(msg: ClientRequest) extends Event
  case class LoadStore(backend: Backend) extends Event
  case class Exit() extends Event
  case class RecoveryEvent() extends Event
  case class HeartbeatEvent() extends Event

  class IOHandler(mgr: StoreManager) extends CompletionHandler {
    override def complete(op: Completion): Unit = {
      mgr.events.add(IOCompletion(op))
    }
  }

  class CRLHandler(mgr: StoreManager) extends SaveCompletionHandler {
    override def saveComplete(op: SaveCompletion): Unit = {
      mgr.events.add(CRLCompletion(op))
    }
  }
}

class StoreManager(val objectCacheFactory: () => ObjectCache,
                   val net: Messenger,
                   val backgroundTasks: BackgroundTask,
                   crlFactory: CrashRecoveryLogFactory,
                   val finalizerFactory: TransactionFinalizer.Factory,
                   val txDriverFactory: TransactionDriver.Factory,
                   val heartbeatPeriod: Duration,
                   initialBackends: List[Backend]) extends Logging {
  import StoreManager._

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)
  private val crlHandler = new CRLHandler(this)

  private val txStatusCache = new TransactionStatusCache()

  private val crl = crlFactory.createCRL(crlHandler)

  private val threadPool = Executors.newFixedThreadPool(1)

  protected var shutdownCalled = false
  private val shutdownPromise: Promise[Unit] = Promise()

  def start(): Unit = {
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        while (!shutdownCalled) {
          var event = events.poll(3, TimeUnit.SECONDS)
          while (event != null) {
            handleEvent(event)
            event = events.poll(0, TimeUnit.SECONDS)
          }
        }
      }
    })
  }

  backgroundTasks.schedulePeriodic(heartbeatPeriod) {
    events.put(HeartbeatEvent())
  }

  protected var stores: Map[StoreId, Store] = initialBackends.map { backend =>
    val store = new Store(backend, objectCacheFactory(), net, backgroundTasks, crl,
      txStatusCache, finalizerFactory, txDriverFactory, heartbeatPeriod*8)

    backend.setCompletionHandler(op => {
      events.add(IOCompletion(op))
    })

    backend.storeId -> store
  }.toMap

  def hasTransactions: Boolean = synchronized {
    stores.valuesIterator.exists(_.hasTransactions)
  }

  def logTransactionStatus(log: String => Unit): Unit = synchronized {
    stores.values.foreach(_.logTransactionStatus(log))
  }

  def loadStore(backend: Backend): Unit = {
    events.put(LoadStore(backend))
  }

  def receiveTransactionMessage(msg: TxMessage): Unit = {
    events.put(TransactionMessage(msg))
  }

  def receiveClientRequest(msg: ClientRequest): Unit = {
    events.put(ClientReq(msg))
  }

  def shutdown()(implicit ec: ExecutionContext): Future[Unit] = {
    events.put(Exit())
    shutdownPromise.future
  }

  protected def addRecoveryEvent(): Unit = events.add(RecoveryEvent())

  /** Placeholder for mixin class to implement transaction and allocation recovery */
  protected def handleRecoveryEvent(): Unit = ()

  def hasEvents: Boolean = {
    events.size() != 0
  }

  /** Handles all events in the event queue. Returns when the queue is empty */
  def handleEvents(): Unit = {
    var event = events.poll(0, TimeUnit.NANOSECONDS)
    while (event != null) {
      handleEvent(event)
      event = events.poll(0, TimeUnit.NANOSECONDS)
    }
  }

  /** Preforms a blocking poll on the event queue, awaitng the delivery of an event */
  protected def awaitEvent(): Unit = {
    handleEvent(events.poll(1, TimeUnit.DAYS))
  }

  private def handleEvent(event: Event): Unit = {
    event match {

      case IOCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.frontend.backendOperationComplete(op)
      }

      case CRLCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.frontend.crlSaveComplete(op)
      }

      case TransactionMessage(msg) =>
        stores.get(msg.to).foreach { store =>
          store.receiveTransactionMessage(msg)
        }

      case ClientReq(msg) => stores.get(msg.toStore).foreach { store =>

        msg match {
          case a: Allocate => store.frontend.allocateObject(a)

          case r: Read =>
            r.objectPointer.getStoreLocater(store.storeId).foreach { locater =>
              store.frontend.readObjectForNetwork(r.fromClient, r.readUUID, locater)
            }

          case op: OpportunisticRebuild => store.frontend.readObjectForOpportunisticRebuild(op)

          case s: TransactionCompletionQuery =>
            val isComplete = txStatusCache.getStatus(s.transactionId) match {
              case None => false
              case Some(e) => e.status match {
                case TransactionStatus.Unresolved => false
                case _ => true
              }
            }
            val r = TransactionCompletionResponse(s.fromClient, s.toStore, s.queryUUID, isComplete)
            net.sendClientResponse(r)
        }
      }

      case RecoveryEvent() =>
        handleRecoveryEvent()

      case LoadStore(backend) =>
        val store = new Store(backend, objectCacheFactory(), net, backgroundTasks, crl,
          txStatusCache,finalizerFactory, txDriverFactory, heartbeatPeriod*8)
        backend.setCompletionHandler(ioHandler)
        stores += (backend.storeId -> store)

      case HeartbeatEvent() =>
        //logger.trace("Main loop got heartbeat event")
        stores.valuesIterator.foreach(_.heartbeat())

      case null => // nothing to do

      case _:Exit =>
        shutdownCalled = true
        shutdownPromise.success(())
    }
  }
}
