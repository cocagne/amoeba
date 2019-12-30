package com.ibm.amoeba.client.internal

import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, ObjectState, ReadError}
import com.ibm.amoeba.common.network.ReadResponse
import com.ibm.amoeba.common.objects.ObjectPointer

import scala.concurrent.Future

trait ReadDriver {
  def readResult: Future[Either[ReadError, ObjectState]]

  /** Called to begin the read process. Read messages must not be sent until this method is called */
  def begin(): Unit

  /** Called to abandon the read. This calls should cancel all activity scheduled for the future */
  def shutdown(): Unit

  /** Returns True when all stores have been heard from */
  def receiveReadResponse(response:ReadResponse): Boolean

  val opportunisticRebuildManager: OpportunisticRebuildManager
}

object ReadDriver {

  /**
    * UUID is the read UUID
    */
  type Factory = (AmoebaClient, ObjectPointer, UUID) => ReadDriver

}
