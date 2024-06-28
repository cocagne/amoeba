package com.ibm.amoeba.client

import com.ibm.amoeba.codec
import com.ibm.amoeba.common.network.Codec

import java.util.UUID

final case class HostId(uuid: UUID) extends AnyVal


object Host:
  def apply(buff: Array[Byte]): Host = Codec.decode(codec.Host.parseFrom(buff))


case class Host(hostId: HostId,
                name: String,
                address: String,
                dataPort: Int,
                cncPort: Int):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray