package com.ibm.amoeba.server.store


import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.ibm.amoeba.common.network.{Allocate, ClientRequest, Read, TxMessage}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.server.crl.{CrashRecoveryLogFactory, SaveCompletion, SaveCompletionHandler}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.{Backend, Completion, CompletionHandler}
import com.ibm.amoeba.server.store.cache.ObjectCache

import scala.concurrent.{ExecutionContext, Future, Promise}

object StoreManager {
  sealed abstract class Event

  case class IOCompletion(op: Completion) extends Event
  case class CRLCompletion(op: SaveCompletion) extends Event
  case class TransactionMessage(msg: TxMessage) extends Event
  case class ClientReq(msg: ClientRequest) extends Event
  case class LoadStore(backend: Backend) extends Event
  case class Exit() extends Event

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

class StoreManager(val objectCache: ObjectCache,
                   val net: Messenger,
                   crlFactory: CrashRecoveryLogFactory,
                   initialBackends: List[Backend]) {
  import StoreManager._

  private val events = new LinkedBlockingQueue[Event]()

  private val ioHandler = new IOHandler(this)
  private val crlHandler = new CRLHandler(this)

  private val crl = crlFactory.createCRL(crlHandler)

  private var exitThread = false
  private val shutdownPromise: Promise[Unit] = Promise()

  private var stores: Map[StoreId, Frontend] = Map()

  private val managerThread = new Thread {
    override def run(): Unit = {
      threadLoop()
    }
  }

  {
    initialBackends.foreach(loadStore)
    managerThread.start()
  }

  def loadStore(backend: Backend): Unit = {
    events.add(LoadStore(backend))
  }

  def receiveTransactionMessage(msg: TxMessage): Unit = {
    events.add(TransactionMessage(msg))
  }

  def receiveClientRequest(msg: ClientRequest): Unit = {
    events.add(ClientReq(msg))
  }

  def shutdown()(implicit ec: ExecutionContext): Future[Unit] = {
    events.add(Exit())
    shutdownPromise.future
  }

  private def threadLoop(): Unit = while (!exitThread) {
    events.poll(5, TimeUnit.MINUTES) match {

      case IOCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.backendOperationComplete(op)
      }

      case CRLCompletion(op) => stores.get(op.storeId).foreach { store =>
        store.crlSaveComplete(op)
      }

      case TransactionMessage(msg) => stores.get(msg.to).foreach { store =>
        store.receiveTransactionMessage(msg)
      }

      case ClientReq(msg) => stores.get(msg.toStore).foreach { store =>
        msg match {
          case a: Allocate => store.allocateObject(a)
          case r: Read =>
            r.objectPointer.getStorePointer(store.storeId).foreach { sp =>
              val locater = Locater(r.objectPointer.id, sp)
              store.readObjectForNetwork(r.fromClient, r.readUUID, locater)
            }
        }
      }

      case LoadStore(backend) =>
        val f = new Frontend(backend.storeId, backend, objectCache, net, crl)
        backend.setCompletionHandler(ioHandler)
        stores += backend.storeId -> f

      case null => // nothing to do

      case _:Exit =>
        exitThread = true
        shutdownPromise.success(())
    }
  }
}
