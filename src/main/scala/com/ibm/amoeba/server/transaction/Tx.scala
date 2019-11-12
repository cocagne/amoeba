package com.ibm.amoeba.server.transaction

import com.ibm.amoeba.common.network.{TxAccept, TxFinalized, TxHeartbeat, TxPrepare, TxResolved, TxStatusRequest}
import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.ReadError
import com.ibm.amoeba.server.crl.TxSaveId
import com.ibm.amoeba.server.store.{CommitError, ObjectState}
import org.apache.logging.log4j.scala.Logging

class Tx extends Logging {

  def receivePrepare(m: TxPrepare): Unit = {

  }

  def receiveAccept(m: TxAccept): Unit = {

  }

  def receiveResolved(m: TxResolved): Unit = {

  }

  def receiveFinalized(m: TxFinalized): Unit = {

  }

  def receiveHeartbeat(m: TxHeartbeat): Unit = {

  }

  def receiveStatusRequest(m: TxStatusRequest): Unit = {

  }

  def objectLoaded(os: ObjectState): Unit = {

  }

  def objectLoadFailed(objectId: ObjectId, err: ReadError.Value): Unit = {

  }

  def crlSaveComplete(saveId: TxSaveId): Unit = {

  }

  def commitComplete(objectId: ObjectId, result: Either[Unit, CommitError.Value]): Unit = {

  }
}
