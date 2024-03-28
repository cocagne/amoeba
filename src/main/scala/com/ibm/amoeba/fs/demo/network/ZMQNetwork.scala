package com.ibm.amoeba.fs.demo.network

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import com.ibm.amoeba.client.internal.network.Messenger as ClientMessenger
import com.ibm.amoeba.server.network.Messenger as ServerMessenger
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.network.{ClientId, ClientRequest, ClientResponse, NetworkCodec, NodeHeartbeat, Read, ReadResponse, TxMessage}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.network.protocol.Message as PMessage
import com.ibm.amoeba.common.objects.{Metadata, ObjectId}
import com.ibm.amoeba.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.ZMQ.{DONTWAIT, PollItem}
import org.zeromq.{SocketType, ZContext, ZMQ}

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

  sealed abstract class SendQueueMsg
  case class SendClientRequest(msg: ClientRequest) extends SendQueueMsg
  case class SendClientTransactionMessage(msg: TxMessage) extends SendQueueMsg
  case class SendClientResponse(msg: ClientResponse) extends SendQueueMsg
  case class SendServerTransactionMessage(msg: TxMessage) extends SendQueueMsg

  class CliMessenger(net: ZMQNetwork) extends ClientMessenger with Logging {
    def sendClientRequest(msg: ClientRequest): Unit = synchronized {
      msg match
        case rr: Read => logger.trace(s"Sending ${msg.getClass.getSimpleName} ${rr.readUUID} to ${msg.toStore}")
        case _ => logger.trace(s"Sending ${msg.getClass.getSimpleName} to ${msg.toStore}")
      net.queueMessageForSend(SendClientRequest(msg))
    }

    def sendTransactionMessage(msg: TxMessage): Unit = synchronized {
      logger.trace(s"Sending TxId ${msg.transactionId} ${msg.getClass.getSimpleName} to ${msg.to}")
      net.queueMessageForSend(SendClientTransactionMessage(msg))
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
  }

  class SrvMessenger(net: ZMQNetwork) extends ServerMessenger with Logging {
    def sendClientResponse(msg: ClientResponse): Unit = synchronized {
      val zaddr = net.clients.get(msg.toClient) match
        case Some(zmqIdentity) => String(zmqIdentity)
        case None => "UNKNOWN!"

      msg match
        case rr: ReadResponse => logger.trace(s"Sending ReadResponse for read ${rr.readUUID} to ${msg.toClient}. ZAddr: $zaddr")
        case _ => logger.trace(s"Sending ${msg.getClass.getSimpleName} to ${msg.toClient}. ZAddr: $zaddr")

      net.queueMessageForSend(SendClientResponse(msg))
    }

    def sendTransactionMessage(msg: TxMessage): Unit = synchronized {
      logger.trace(s"Sending TxId ${msg.transactionId} ${msg.getClass.getSimpleName} to ${msg.to}")
      net.queueMessageForSend(SendServerTransactionMessage(msg))
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

  logger.debug(s"ZMQNetwork Client ID: ${clientId.uuid.toString}")

  private val context = new ZContext()

  private var clients: Map[ClientId, Array[Byte]] = Map()

  private var lastRouterMessageReceived = System.nanoTime()

  private var nodeStates: Map[String, NodeState] = nodes.map{t => t._1 -> new NodeState(t._1, 0, false)}

  private val sendQueueSocket = context.createSocket(SocketType.DEALER)
  sendQueueSocket.bind("inproc://send-message-queued")

  private val sendQueue = new java.util.concurrent.ConcurrentLinkedQueue[SendQueueMsg]()

  private val sendQueueClientSocket = ThreadLocal.withInitial[ZMQ.Socket]: () =>
    val socket = context.createSocket(SocketType.DEALER)
    socket.connect("inproc://send-message-queued")
    socket

  private val sendQueuePollItem = new PollItem(sendQueueSocket, ZMQ.Poller.POLLIN)

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

  case class Peer(nodeName: String, dealer: ZMQ.Socket, pollItem: PollItem)

  private val dealers = nodes.map { t =>
    val (nodeName, (host, port)) = t
    val dealer = context.createSocket(SocketType.DEALER)
    dealer.setIdentity(clientId.toBytes)
    dealer.connect(s"tcp://$host:$port")
    heartbeatMessage.foreach(msg => dealer.send(msg))
    Peer(nodeName, dealer, new PollItem(dealer, ZMQ.Poller.POLLIN))
  }.toArray

  private val peers = dealers.map(p => p.nodeName -> p).toMap

  private val routerPollItem = routerSocket.map { router =>
    new PollItem(router, ZMQ.Poller.POLLIN)
  }

  def clientMessenger: ClientMessenger = new CliMessenger(this)

  def serverMessenger: ServerMessenger = new SrvMessenger(this)

  // Queue message in concurrent linked list and send an empty message to the queue socket
  // to wake the IO thread if it's sleeping in a call to poll()
  def queueMessageForSend(msg: SendQueueMsg): Unit =
    sendQueue.add(msg)
    sendQueueClientSocket.get().send("")

  private def heartbeat(): Unit = {
    val offlineThreshold = System.nanoTime() - (heartbeatPeriod * 3).toNanos
    nodeStates.foreach { t =>
      val (_, ns) = t
      if (ns.lastHeartbeatTime <= offlineThreshold && ns.isOnline) {
        ns.setOffline()
      }
    }
    heartbeatMessage.foreach { msg =>
      logger.trace("Sending node heartbeat to peers")
      dealers.foreach(_.dealer.send(msg))
    }
  }

  def ioThread(): Unit = {

    val (size, routerSocketIndex) = routerPollItem match
      case Some(_) => (dealers.length + 2, dealers.length + 1)
      case None => (dealers.length + 1, -1)

    var poller = context.createPoller(size)

    dealers.foreach(peer => poller.register(peer.pollItem))
    poller.register(sendQueuePollItem)
    routerPollItem.foreach(poller.register)

    val heartBeatPeriodMillis = heartbeatPeriod.toMillis.asInstanceOf[Int]
    var nextHeartbeat = System.currentTimeMillis() + heartBeatPeriodMillis

    while (!Thread.currentThread().isInterrupted) {
      val now = System.currentTimeMillis()

      if (now >= nextHeartbeat) {
        nextHeartbeat = now + heartBeatPeriodMillis
        heartbeat()
      }

      try {
        val timeToNextHB = nextHeartbeat - now
        if timeToNextHB > 0 then
          //logger.trace(s"*** SLEEPING. Time to next HB: $timeToNextHB")
          poller.poll(timeToNextHB)
          //logger.trace(s"*** Woke from poll. Time to next HB: ${nextHeartbeat - System.currentTimeMillis()}")
      } catch {
        case e: Throwable =>
          logger.warn(s"Poll method threw an exception. Creating a new poller. Error: $e")

          dealers.foreach(peer => poller.unregister(peer.dealer))
          routerSocket.foreach(poller.unregister)

          poller = context.createPoller(dealers.length + 1)

          dealers.foreach(peer => poller.register(peer.pollItem))
          routerPollItem.foreach(poller.register)
      }

      for (i <- dealers.indices) {
        if (poller.pollin(i)) {
          var msg = dealers(i).dealer.recv(ZMQ.DONTWAIT)
          while msg != null do
            try
              onDealerMessageReceived(msg)
            catch
              case t: Throwable => logger.error(s"**** Error in onDealerMessageReceived: $t", t)
            msg = dealers(i).dealer.recv(ZMQ.DONTWAIT)
        }
      }

      routerSocket.foreach { router =>
        if (poller.pollin(routerSocketIndex)) {
          var from = router.recv(ZMQ.DONTWAIT)
          var msg = router.recv(ZMQ.DONTWAIT)
          while from != null && msg != null do
            try
              onRouterMessageReceived(from, msg)
            catch
              case t: Throwable => logger.error(s"**** Error in onRouterMessageReceived: $t", t)
            from = router.recv(ZMQ.DONTWAIT)
            msg = router.recv(ZMQ.DONTWAIT)
        }
      }

      if poller.pollin(dealers.length) then
        var msg = sendQueueSocket.recv(ZMQ.DONTWAIT)
        while msg != null do
          msg = sendQueueSocket.recv(ZMQ.DONTWAIT)

      var qmsg = sendQueue.poll()
      while qmsg != null do
        qmsg match
          case SendClientRequest(msg) =>
            logger.trace(s"Sending ${msg.getClass.getSimpleName} to ${msg.toStore}")
            peers(stores(msg.toStore)).dealer.send(MessageEncoder.encodeMessage(msg))
          case SendClientTransactionMessage(msg) =>
            logger.trace(s"Sending ${msg.getClass.getSimpleName} to ${msg.to}")
            peers(stores(msg.to)).dealer.send(MessageEncoder.encodeMessage(msg))
          case SendClientResponse(msg) =>
            logger.trace(s"Sending ${msg.getClass.getSimpleName} to ${msg.toClient}")
            clients.get(msg.toClient).foreach: zmqIdentity =>
              routerSocket.foreach: router =>
                router.send(zmqIdentity, ZMQ.SNDMORE)
                router.send(MessageEncoder.encodeMessage(msg))
          case SendServerTransactionMessage(msg) =>
            logger.trace(s"Sending ${msg.getClass.getSimpleName} to ${msg.to}")
            peers(stores(msg.to)).dealer.send(MessageEncoder.encodeMessage(msg))
        qmsg = sendQueue.poll()
    }

    logger.trace("ZMQNetwork.enterEventLoop EXITING")
  }

  private def onDealerMessageReceived(msg: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(msg)

    val msgLen = bb.getInt()

    val p = try PMessage.getRootAsMessage(bb.asReadOnlyBuffer()) catch
      case t: Throwable =>
        logger.error(s"******* PARSE DEALER MESSAGE ERROR: $t", t)
        throw t

    if (p.readResponse() != null) {
      val message = NetworkCodec.decode(p.readResponse())
      logger.trace(s"Got ReadResponse for read ${message.readUUID} from store ${message.fromStore}")
      onClientResponseReceived(message)
    }
    else if (p.txResolved() != null) {
      val message = NetworkCodec.decode(p.txResolved())
      logger.trace(s"Got TxResolved for TxId ${message.transactionId} from store ${message.fromStore}. Committed: ${message.committed}")
      onClientResponseReceived(message)
    }
    else if (p.txFinalized() != null) {
      val message = NetworkCodec.decode(p.txFinalized())
      logger.trace(s"Got TxFinalized for TxId ${message.transactionId} from store ${message.fromStore}. Committed: ${message.committed}")
      onClientResponseReceived(message)
    }
    else if (p.allocateResponse() != null) {
      val message = NetworkCodec.decode(p.allocateResponse())
      logger.trace(s"Got AllocateResponse from ${message.fromStore} obj ${message.newObjectId}")
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

    lastRouterMessageReceived = System.nanoTime()

    val msgLen = bb.getInt()

    // Must pass a read-only copy to the following method. It'll corrupt the rest of the buffer otherwise
    val p = try PMessage.getRootAsMessage(bb.asReadOnlyBuffer()) catch
      case t: Throwable =>
        logger.error(s"******* PARSE ROUTER MESSAGE ERROR: $t", t)
        throw t

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
      logger.trace(s"Got Read ${message.readUUID} for object ${message.objectPointer.id} from ${message.fromClient}")
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
      logger.trace(s"Got Prepare ${message.txd.transactionId} from ${message.from}. ProposalId:${message.proposalId}")
      onTransactionMessageReceived(message)
    }
    else if (p.prepareResponse() != null) {
      //println("got prepareResponse")
      val message = NetworkCodec.decode(p.prepareResponse())
      logger.trace(s"Got PrepareResponse ${message.transactionId} from ${message.from}. ProposalId:${message.proposalId} Disposition ${message.disposition}")
      onTransactionMessageReceived(message)
    }
    else if (p.accept() != null) {
      //println("got accept")
      val message = NetworkCodec.decode(p.accept())
      logger.trace(s"Got Accept ${message.transactionId} from ${message.from}. ProposalId:${message.proposalId}. ${message.value}")
      onTransactionMessageReceived(message)
    }
    else if (p.acceptResponse() != null) {
      //println("got acceptResponse")
      val message = NetworkCodec.decode(p.acceptResponse())
      logger.trace(s"Got AcceptResponse ${message.transactionId} from ${message.from}. ProposalId:${message.proposalId} Response: ${message.response}")
      onTransactionMessageReceived(message)
    }
    else if (p.resolved() != null) {
      val message = NetworkCodec.decode(p.resolved())
      logger.trace(s"Got Resolved ${message.transactionId} from ${message.from}. Committed: ${message.committed}")
      //println(s"got resolved for txid ${message.transactionUUID} committed = ${message.committed}")
      onTransactionMessageReceived(message)
    }
    else if (p.committed() != null) {
      val message = NetworkCodec.decode(p.committed())
      logger.trace(s"Got Committed ${message.transactionId} from ${message.from}")
      //println(s"got committed for txid ${message.transactionUUID}")
      onTransactionMessageReceived(message)
    }
    else if (p.finalized() != null) {
      //println("got finalized")
      val message = NetworkCodec.decode(p.finalized())
      logger.trace(s"Got Finalized ${message.transactionId} from ${message.from}. Committed: ${message.committed}")
      onTransactionMessageReceived(message)
    }
    else if (p.heartbeat() != null) {
      val message = NetworkCodec.decode(p.heartbeat())
      logger.trace(s"Got TxHeartbeat ${message.transactionId} from ${message.from}")
      onTransactionMessageReceived(message)
    }
    else if (p.allocate() != null) {
      //println(s"got allocate request. Receiver: $a")
      val message = NetworkCodec.decode(p.allocate())
      logger.trace(s"Got Allocate ${message.allocationTransactionId} ${message.newObjectId}.")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else if (p.opportunisticRebuild() != null) {
      val message = NetworkCodec.decode(p.opportunisticRebuild())
      logger.trace(s"Got OpportunisticRebuild for object ${message.pointer.id}")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else if (p.transactionCompletionQuery() != null) {
      val message = NetworkCodec.decode(p.transactionCompletionQuery())
      logger.trace(s"Got CompletionQuery for transaction ${message.transactionId}")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)
    }
    else {
      logger.error("Unknown Message!")
    }
  }
}
