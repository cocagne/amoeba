package com.ibm.amoeba.fs.demo.netty_network

import io.netty.channel.{ChannelHandler, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}

object AmoebaChannelInitializer {
  val MaxFrameSize: Int = 1024 * 1024 * 101 // max is 100 MB
}

abstract class AmoebaChannelInitializer extends ChannelInitializer[SocketChannel] {
  import AmoebaChannelInitializer._

  def newChannel(): ChannelHandler

  override def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()

    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MaxFrameSize, 0, 4, 0, 4))
    p.addLast("bytesDecoder", new ByteArrayDecoder())

    p.addLast("frameEncoder", new LengthFieldPrepender(4))
    p.addLast("bytesEncoder", new ByteArrayEncoder())
    p.addLast("aspenChannel", newChannel())
  }
}
