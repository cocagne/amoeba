package com.ibm.amoeba.common.network

import java.util.UUID

case class ClientId(uuid: UUID) extends AnyVal

object ClientId {
  val Null = new ClientId(new UUID(0, 0))
}
