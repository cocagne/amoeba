package com.ibm.amoeba.fs.demo.netty_network

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.network.{ClientResponse, NetworkCodec, TxMessage}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.fs.demo.ConfigFile
import org.apache.logging.log4j.scala.Logging
import com.ibm.amoeba.server.network.{Messenger => ServerMessenger}
import com.ibm.amoeba.common.network.protocol.{Message => PMessage}
import com.ibm.amoeba.common.objects.{Metadata, ObjectId}
import com.ibm.amoeba.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}
import com.ibm.amoeba.server.StoreManager

class StoreNetwork(val nodeName: String,
                   val nnet: NettyNetwork) extends ServerMessenger with Logging {

  import MessageEncoder._

  private[this] var ostoreManager: Option[StoreManager] = None

  private[this] var oclient: Option[AmoebaClient] = None

  def client: Option[AmoebaClient] = synchronized(oclient)

  def setClient(s: AmoebaClient): Unit = synchronized { oclient = Some(s) }

  def setStoreManager(smgr: StoreManager): Unit = synchronized { ostoreManager = Some(smgr) }

  val nodeConfig: ConfigFile.StorageNode = nnet.config.nodes(nodeName)

  val connectionMgr = new StoreConnectionManager(this, nodeConfig.endpoint.port)

  val onlineTracker = new OnlineTracker(nnet.config)

  val stores: Map[StoreId, ClientConnection] = nnet.config.nodes.foldLeft(Map[StoreId, ClientConnection]()) { (m, n) =>
    val ep = n._2.endpoint
    val cnet = new ClientConnection(nnet.clientWorkerGroup, nodeConfig.uuid, n._2.uuid, ep.host, ep.port, _ => (), onlineTracker)

    n._2.stores.foldLeft(m) { (m, s) =>
      m + (StoreId(PoolId(nnet.config.pools(s.pool).uuid), s.store.asInstanceOf[Byte]) -> cnet)
    }
  }

  def receiveMessage(msg: Array[Byte]): Unit = synchronized {
    val bb = ByteBuffer.wrap(msg)
    val origLimit = bb.limit()
    val msgLen = bb.getInt()

    bb.limit(4+msgLen) // Limit to end of message

    // Must pass a read-only copy to the following method. It'll corrupt the rest of the buffer otherwise
    val p = PMessage.getRootAsMessage(bb.asReadOnlyBuffer())

    bb.limit(origLimit)
    bb.position(4 + msgLen) // reposition to encoded data

    if (p.read() != null) {
      val message = NetworkCodec.decode(p.read())
      logger.trace(s"Read request for object ${message.objectPointer.id} from ${message.fromClient}")
      ostoreManager.foreach(_.receiveClientRequest(message))
    }
    else if (p.prepare() != null) {
      //println("got prepare")
      logger.trace(s"Got Prepare message")
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
      logger.trace(s"Tx ${message.txd.transactionId} Prepare message")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.prepareResponse() != null) {
      //println("got prepareResponse")
      val message = NetworkCodec.decode(p.prepareResponse())
      logger.trace(s"Tx ${message.transactionId} PrepareResponse")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.accept() != null) {
      //println("got accept")
      val message = NetworkCodec.decode(p.accept())
      logger.trace(s"Tx ${message.transactionId} Accept")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.acceptResponse() != null) {
      //println("got acceptResponse")
      val message = NetworkCodec.decode(p.acceptResponse())
      logger.trace(s"Tx ${message.transactionId} AcceptResponse")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.resolved() != null) {
      val message = NetworkCodec.decode(p.resolved())
      logger.trace(s"Tx ${message.transactionId} Resolved")
      //println(s"got resolved for txid ${message.transactionUUID} committed = ${message.committed}")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.committed() != null) {
      val message = NetworkCodec.decode(p.committed())
      logger.trace(s"Tx ${message.transactionId} Committed")
      //println(s"got committed for txid ${message.transactionUUID}")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.finalized() != null) {
      //println("got finalized")
      val message = NetworkCodec.decode(p.finalized())
      logger.trace(s"Tx ${message.transactionId} Finalized")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.heartbeat() != null) {
      val message = NetworkCodec.decode(p.heartbeat())
      logger.trace(s"Tx ${message.transactionId} Heartbeat")
      ostoreManager.foreach( _.receiveTransactionMessage(message))
    }
    else if (p.allocate() != null) {
      //println(s"got allocate request. Receiver: $a")
      val message = NetworkCodec.decode(p.allocate())
      logger.trace(s"Tx ${message.allocationTransactionId} Allocate ${message.newObjectId}")
      ostoreManager.foreach( _.receiveClientRequest(message))
    }
    else if (p.opportunisticRebuild() != null) {
      val message = NetworkCodec.decode(p.opportunisticRebuild())
      logger.trace(s"Tx ${message.pointer.id} OpportunisticRebuild")
      ostoreManager.foreach( _.receiveClientRequest(message))
    }
    else if (p.transactionCompletionQuery() != null) {
      val message = NetworkCodec.decode(p.transactionCompletionQuery())
      logger.trace(s"Tx ${message.transactionId} CompletionQuery")
      ostoreManager.foreach( _.receiveClientRequest(message))
    }
    else {
      logger.error("Unknown Message!")
    }
  }

  def sendClientResponse(msg: ClientResponse): Unit = {
    connectionMgr.sendMessageToClient(msg.toClient, encodeMessage(msg))
  }

  def sendTransactionMessage(msg: TxMessage): Unit = {
    stores(msg.to).send(encodeMessage(msg))
  }

  def sendTransactionMessages(msg: List[TxMessage]): Unit = {
    msg.foreach(m => sendTransactionMessage(m))
  }

}
