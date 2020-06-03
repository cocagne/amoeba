package com.ibm.amoeba.fs.demo.network

import java.nio.ByteBuffer

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.client.internal.network.{Messenger => ClientMessenger}
import com.ibm.amoeba.common.network.protocol.{Message => PMessage}
import com.ibm.amoeba.common.network._
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import org.apache.logging.log4j.scala.Logging

class ClientNetwork(val nnet: NettyNetwork,
                    val clientId: ClientId) extends ClientMessenger with Logging {

  import MessageEncoder._

  private[this] var oclient: Option[AmoebaClient] = None

  def client: Option[AmoebaClient] = synchronized(oclient)

  def setClient(s: AmoebaClient): Unit = synchronized { oclient = Some(s) }

  val onlineTracker = new OnlineTracker(nnet.config)

  val stores: Map[StoreId, ClientConnection] = nnet.config.nodes.foldLeft(Map[StoreId, ClientConnection]()) { (m, n) =>
    val ep = n._2.endpoint
    val cnet = new ClientConnection(nnet.clientWorkerGroup, clientId.uuid, n._2.uuid, ep.host, ep.port, receiveMessage, onlineTracker)

    n._2.stores.foldLeft(m) { (m, s) =>
      m + (StoreId(PoolId(nnet.config.pools(s.pool).uuid), s.store.asInstanceOf[Byte]) -> cnet)
    }
  }


  def receiveMessage(arr: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(arr)
    val origLimit = bb.limit()
    val msgLen = bb.getInt()

    bb.limit(4+msgLen) // Limit to end of message

    val p = PMessage.getRootAsMessage(bb)

    if (p.readResponse() != null) {
      val message = NetworkCodec.decode(p.readResponse())
      client.foreach(_.receiveClientResponse(message))
    }
    else if (p.txResolved() != null) {
      val message = NetworkCodec.decode(p.txResolved())
      client.foreach(_.receiveClientResponse(message))
    }
    else if (p.txFinalized() != null) {
      val message = NetworkCodec.decode(p.txFinalized())
      client.foreach(_.receiveClientResponse(message))
    }
    else if (p.allocateResponse() != null) {
      val message = NetworkCodec.decode(p.allocateResponse())
      client.foreach(_.receiveClientResponse(message))
    }
  }

  override def sendClientRequest(msg: ClientRequest): Unit = {
    val bytes = msg match {
      case r: Read => encodeMessage(r)
      case r: TransactionCompletionQuery => encodeMessage(r)
      case r: OpportunisticRebuild => encodeMessage(r)
      case r: Allocate => encodeMessage(r)
    }
    stores.get(msg.toStore).foreach(_.send(bytes))
  }

  override def sendTransactionMessage(msg: TxMessage): Unit = {
    val obytes = msg match {
      case r: TxPrepare => Some(encodeMessage(r))
      case _ => None
    }
    obytes.foreach(bytes => stores.get(msg.to).foreach(_.send(bytes)))
  }

  override def sendTransactionMessages(msg: List[TxMessage]): Unit = {
    msg.foreach(sendTransactionMessage)
  }
}
