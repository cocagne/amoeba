package com.ibm.amoeba.client.internal.read

import java.util.UUID

import com.ibm.amoeba.client.{ObjectState, ReadError}
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.network.ReadResponse
import com.ibm.amoeba.common.objects.ObjectPointer
import com.ibm.amoeba.common.store.StoreId

trait ObjectReader {
  val pointer: ObjectPointer

  val allStores: Set[StoreId] = pointer.hostingStores.toSet

  def receivedResponseFrom(storeId: StoreId): Boolean

  def noResponses: Set[StoreId]

  def rereadCandidates: Map[StoreId, HLCTimestamp]

  def result: Option[Either[ReadError, ObjectState]]

  def receiveReadResponse(response:ReadResponse): Option[Either[ReadError, ObjectState]]

  def numResponses: Int

  def receivedResponsesFromAllStores: Boolean = numResponses == pointer.ida.width

  def debugLogStatus(readUUID: UUID, header: String, log: String => Unit): Unit
}

