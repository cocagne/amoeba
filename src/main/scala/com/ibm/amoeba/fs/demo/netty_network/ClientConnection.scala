package com.ibm.amoeba.fs.demo.netty_network

import java.util.UUID
import java.util.concurrent.{Callable, TimeUnit}

import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelFuture, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.apache.logging.log4j.scala.Logging

class ClientConnection(
                         val clientWorkerGroup: NioEventLoopGroup,
                         val clientUUID: UUID,
                         val hostUUID: UUID,
                         val host: String,
                         val port: Int,
                         val msgReceived: Array[Byte] => Unit,
                         val onlineTracker: OnlineTracker) extends Logging {

  private[this] var octx: Option[ChannelHandlerContext] = None

  private def setContext(o: Option[ChannelHandlerContext]): Unit = synchronized { octx = o }

  private val clientBootstrap = new Bootstrap

  def send(msg: Array[Byte]): Unit = synchronized {
    octx.foreach(_.writeAndFlush(msg))
  }

  private def reconnect(): Unit = {
    logger.info(s"Connecting to $host:$port")

    clientBootstrap.connect(host, port).addListener( (future: ChannelFuture) => {
      if (!future.isSuccess)
        clientWorkerGroup.schedule(new Callable[Unit] { def call(): Unit = reconnect() }, 3, TimeUnit.SECONDS)
    })
  }

  private class ClientChannelHandler() extends ChannelInboundHandlerAdapter {

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      logger.info(s"Connected to $host:$port")
      onlineTracker.setNodeOnline(hostUUID)
      setContext(Some(ctx))
      ctx.writeAndFlush(com.ibm.amoeba.common.util.uuid2byte(clientUUID))
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      logger.info(s"Disconnected from $host:$port")
      onlineTracker.setNodeOffline(hostUUID)
      setContext(None)
      reconnect()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      // Close the connection when an exception is raised.
      cause.printStackTrace()
      ctx.close()
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
      case arr: Array[Byte] => msgReceived(arr)

      case x => logger.error(s"RECEIVED UNEXPECTED MESSAGE TYPE: $x")
    }
  }

  // Initialize connection
  clientBootstrap.group(clientWorkerGroup)
    .channel(classOf[NioSocketChannel])
    .handler(new AmoebaChannelInitializer {
      def newChannel() = new ClientChannelHandler
    })
  reconnect()
}
