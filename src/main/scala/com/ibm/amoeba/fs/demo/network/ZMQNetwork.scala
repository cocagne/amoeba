package com.ibm.amoeba.fs.demo.network

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.ibm.amoeba.client.internal.network.{Messenger => ClientMessenger}
import com.ibm.amoeba.server.network.{Messenger => ServerMessenger}
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.network.{ClientId, ClientRequest, ClientResponse, NetworkCodec, NodeHeartbeat, TxMessage}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.network.protocol.{Message => PMessage}
import com.ibm.amoeba.common.objects.{Metadata, ObjectId}
import com.ibm.amoeba.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.ZMQ.PollItem
import org.zeromq.{SocketType, ZContext, ZLoop, ZMQ}

import scala.concurrent.duration.Duration

object ZMQNetwork {
  class SocketState(val nodeName: String,
                    var dealer: ZMQ.Socket)

  class NodeState(val nodeName: String,
                  var lastHeartbeatTime: Long,
                  var isOnline: Boolean) extends Logging {

    def heartbeatReceived(): Unit = {
      lastHeartbeatTime = System.nanoTime()
      if (!isOnline)
        logger.info(s"Node $nodeName is Online")
      isOnline = true
    }
    def setOffline(): Unit = {
      if (isOnline)
        logger.info(s"Node $nodeName is Offline")
      isOnline = false
    }
  }

  class CliMessenger(net: ZMQNetwork) extends ClientMessenger with Logging {
    def sendClientRequest(msg: ClientRequest): Unit = synchronized {
      net.dealers(net.stores(msg.toStore)).send(MessageEncoder.encodeMessage(msg))
    }

    def sendTransactionMessage(msg: TxMessage): Unit = synchronized {
      logger.trace(s"Sending TxId ${msg.transactionId} ${msg.getClass.getSimpleName} to ${msg.to}")
      net.dealers(net.stores(msg.to)).send(MessageEncoder.encodeMessage(msg))
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
  }

  class SrvMessenger(net: ZMQNetwork) extends ServerMessenger with Logging {
    def sendClientResponse(msg: ClientResponse): Unit = synchronized {
      net.clients.get(msg.toClient).foreach { zmqIdentity =>
        net.routerSocket.foreach { router =>
          router.send(zmqIdentity, ZMQ.SNDMORE)
          router.send(MessageEncoder.encodeMessage(msg))
        }
      }
    }

    def sendTransactionMessage(msg: TxMessage): Unit = synchronized {
      logger.trace(s"Sending TxId ${msg.transactionId} ${msg.getClass.getSimpleName} to ${msg.to}")
      net.dealers(net.stores(msg.to)).send(MessageEncoder.encodeMessage(msg))
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
  }
}

class ZMQNetwork(val oclientId: Option[ClientId],
                 val nodes: Map[String, (String, Int)], // NodeName -> (hostname, port)
                 val stores: Map[StoreId, String], // StoreId -> NodeName
                 val storageNode: Option[(String, String, Int)], // (NodeName, hostname/IP, port)
                 val heartbeatPeriod: Duration,
                 val onClientResponseReceived: ClientResponse => Unit,
                 val onClientRequestReceived: ClientRequest => Unit,
                 val onTransactionMessageReceived: TxMessage => Unit) extends Logging {

  import ZMQNetwork._

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))

  private val context = new ZContext()

  private val zloop = new ZLoop(context)

  private var clients: Map[ClientId, Array[Byte]] = Map()

  private var nodeStates: Map[String, NodeState] = nodes.map{t => t._1 -> new NodeState(t._1, 0, false)}

  private val routerSocket = storageNode.map { t =>
    val (_, _, port) = t
    val router = context.createSocket(SocketType.ROUTER)
    router.bind(s"tcp://*:$port")
    router
  }

  private val heartbeatMessage = storageNode.map { t =>
    val (nodeName, _, _) = t
    MessageEncoder.encodeMessage(NodeHeartbeat(nodeName))
  }

  private var dealers = nodes.map { t =>
    val (nodeName, (host, port)) = t
    val dealer = context.createSocket(SocketType.DEALER)
    dealer.connect(s"tcp://$host:$port")
    heartbeatMessage.foreach(msg => dealer.send(msg))
    nodeName -> dealer
  }

  private var pollItems = dealers.map(t => t._1 -> new PollItem(t._2, ZMQ.Poller.POLLIN))

  def clientMessenger: ClientMessenger = new CliMessenger(this)

  def serverMessenger: ServerMessenger = new SrvMessenger(this)

  private object DealerHandler extends ZLoop.IZLoopHandler {
    override def handle(loop: ZLoop, item: PollItem, arg: Object): Int = {
      val msg = item.getSocket.recv()
      onDealerMessageReceived(msg)
      0
    }
  }

  private def heartbeat(): Unit = {
    val offlineThreshold = System.nanoTime() - (heartbeatPeriod * 3).toNanos
    nodeStates.foreach { t =>
      val (_, ns) = t
      if (ns.lastHeartbeatTime <= offlineThreshold && ns.isOnline) {
        ns.setOffline()
        // Close the dealer socket and set up a new one
        logger.trace(s"Attempting to reconnect to offline peer: ${ns.nodeName}")
        val old_pi = pollItems(ns.nodeName)
        val old_dealer = dealers(ns.nodeName)
        zloop.removePoller(old_pi)
        old_dealer.close()
        val newDealer = context.createSocket(SocketType.DEALER)
        val (host, port) = nodes(ns.nodeName)
        newDealer.connect(s"tcp://$host:$port")
        heartbeatMessage.foreach(msg => newDealer.send(msg))
        val newPi = new PollItem(newDealer, ZMQ.Poller.POLLIN)
        zloop.addPoller(newPi, DealerHandler, null)
        pollItems += ns.nodeName -> newPi
        dealers += ns.nodeName -> newDealer
      }
    }
    heartbeatMessage.foreach { msg =>
      logger.trace("Sending node heartbeat to peers")
      dealers.valuesIterator.foreach(_.send(msg))
    }
  }

  def enterEventLoop(): Unit = {

    object RouterHandler extends ZLoop.IZLoopHandler {
      override def handle(loop: ZLoop, item: PollItem, arg: Object): Int = {
        val from = item.getSocket.recv()
        val msg = item.getSocket.recv()
        onRouterMessageReceived(from, msg)
        0
      }
    }

    object HeartbeatTimerHandler extends ZLoop.IZLoopHandler {
      override def handle(loop: ZLoop, item: PollItem, arg: Object): Int = {
        heartbeat()
        0
      }
    }

    pollItems.valuesIterator.foreach { zloop.addPoller(_, DealerHandler, null) }

    routerSocket.foreach { router =>
      zloop.addPoller(new PollItem(router, ZMQ.Poller.POLLIN), RouterHandler, null)
      zloop.addTimer(heartbeatPeriod.toMillis.asInstanceOf[Int], 0, HeartbeatTimerHandler, null)
    }

    zloop.start()
  }

  private def onDealerMessageReceived(msg: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(msg)

    val msgLen = bb.getInt()

    val p = PMessage.getRootAsMessage(bb)

    if (p.readResponse() != null) {
      val message = NetworkCodec.decode(p.readResponse())
      logger.trace(s"Got read response for read ${message.readUUID} from store ${message.fromStore}")
      onClientResponseReceived(message)
    }
    else if (p.txResolved() != null) {
      val message = NetworkCodec.decode(p.txResolved())
      onClientResponseReceived(message)
    }
    else if (p.txFinalized() != null) {
      val message = NetworkCodec.decode(p.txFinalized())
      onClientResponseReceived(message)
    }
    else if (p.allocateResponse() != null) {
      val message = NetworkCodec.decode(p.allocateResponse())
      logger.trace(s"*** Allocate response from ${message.fromStore} obj ${message.newObjectId}")
      onClientResponseReceived(message)
    }
  }

  private def updateClientId(clientId: ClientId, routerAddress: Array[Byte]): Unit = {
    clients.get(clientId) match {
      case None => clients += clientId -> routerAddress
      case Some(addr) =>
        if (!java.util.Arrays.equals(routerAddress, addr))
          clients += clientId -> routerAddress
    }
  }

  private def onRouterMessageReceived(from: Array[Byte], msg: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)

    val msgLen = bb.getInt()

    // Must pass a read-only copy to the following method. It'll corrupt the rest of the buffer otherwise
    val p = PMessage.getRootAsMessage(bb.asReadOnlyBuffer())

    if (p.nodeHeartbeat() != null) {
      val msg = NetworkCodec.decode(p.nodeHeartbeat())
      logger.trace(s"Got heartbeat from node ${msg.nodeName}")
      nodeStates.get(msg.nodeName) match {
        case None =>
          val ns = new NodeState(msg.nodeName,0, false)
          ns.heartbeatReceived()
          nodeStates += msg.nodeName -> ns
        case Some(ns) => ns.heartbeatReceived()
      }
    }
    else if (p.read() != null) {
      val message = NetworkCodec.decode(p.read())
      logger.trace(s"Read requestid ${message.readUUID} for object ${message.objectPointer.id} from ${message.fromClient}")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else if (p.prepare() != null) {
      bb.position(4 + msgLen)
      val contentSize = bb.getInt()
      val preTxSize = bb.getInt()

      val contentEndPos = bb.position() + contentSize
      val preTxEndPos = contentEndPos + preTxSize

      //val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
      //println(s"got prepare txid ${message.txd.transactionUUID} Leader ${message.txd.designatedLeaderUID} for objects: ${sb.toString()}")

      val updateContent = if (bb.remaining() == 0) (Nil, Nil) else {

        var localUpdates: List[ObjectUpdate] = Nil
        var preTxRebuilds: List[PreTransactionOpportunisticRebuild] = Nil

        // local update content is a series of <16-byte-uuid><4-byte-length><data>

        while (bb.position() != contentEndPos) {
          val msb = bb.getLong()
          val lsb = bb.getLong()
          val len = bb.getInt()
          val uuid = new UUID(msb, lsb)

          val slice = bb.asReadOnlyBuffer()

          slice.limit( slice.position() + len )
          bb.position( bb.position() + len )
          localUpdates = ObjectUpdate(ObjectId(uuid), DataBuffer(slice)) :: localUpdates
        }

        // PreTx Rebuilds are a series of <16-byte-uuid><encoded-object-metadata><4-byte-length><data>

        while (bb.position() != preTxEndPos) {
          val msb = bb.getLong()
          val lsb = bb.getLong()
          val uuid = new UUID(msb, lsb)
          val metadata = Metadata(bb)
          val len = bb.getInt()

          val slice = bb.asReadOnlyBuffer()
          slice.limit( slice.position() + len )
          bb.position( bb.position() + len )
          preTxRebuilds = PreTransactionOpportunisticRebuild(ObjectId(uuid), metadata, DataBuffer(slice)) :: preTxRebuilds
        }

        (localUpdates, preTxRebuilds)
      }
      val message = NetworkCodec.decode(p.prepare(), updateContent._1, updateContent._2)
      logger.trace(s"Tx ${message.txd.transactionId} Prepare message from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.prepareResponse() != null) {
      //println("got prepareResponse")
      val message = NetworkCodec.decode(p.prepareResponse())
      logger.trace(s"Tx ${message.transactionId} PrepareResponse from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.accept() != null) {
      //println("got accept")
      val message = NetworkCodec.decode(p.accept())
      logger.trace(s"Tx ${message.transactionId} Accept from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.acceptResponse() != null) {
      //println("got acceptResponse")
      val message = NetworkCodec.decode(p.acceptResponse())
      logger.trace(s"Tx ${message.transactionId} AcceptResponse from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.resolved() != null) {
      val message = NetworkCodec.decode(p.resolved())
      logger.trace(s"Tx ${message.transactionId} Resolved from ${message.from}")
      //println(s"got resolved for txid ${message.transactionUUID} committed = ${message.committed}")
      onTransactionMessageReceived(message)
    }
    else if (p.committed() != null) {
      val message = NetworkCodec.decode(p.committed())
      logger.trace(s"Tx ${message.transactionId} Committed from ${message.from}")
      //println(s"got committed for txid ${message.transactionUUID}")
      onTransactionMessageReceived(message)
    }
    else if (p.finalized() != null) {
      //println("got finalized")
      val message = NetworkCodec.decode(p.finalized())
      logger.trace(s"Tx ${message.transactionId} Finalized from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.heartbeat() != null) {
      val message = NetworkCodec.decode(p.heartbeat())
      logger.trace(s"Tx ${message.transactionId} Heartbeat from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.allocate() != null) {
      //println(s"got allocate request. Receiver: $a")
      val message = NetworkCodec.decode(p.allocate())
      logger.trace(s"Tx ${message.allocationTransactionId} Allocate ${message.newObjectId}")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else if (p.opportunisticRebuild() != null) {
      val message = NetworkCodec.decode(p.opportunisticRebuild())
      logger.trace(s"Tx ${message.pointer.id} OpportunisticRebuild")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else if (p.transactionCompletionQuery() != null) {
      val message = NetworkCodec.decode(p.transactionCompletionQuery())
      logger.trace(s"Tx ${message.transactionId} CompletionQuery")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else {
      logger.error("Unknown Message!")
    }
  }
}
