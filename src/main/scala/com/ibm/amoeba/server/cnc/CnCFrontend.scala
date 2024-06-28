package com.ibm.amoeba.server.cnc

import scala.concurrent.Future


trait CnCFrontend:
  def send(msg: NewStore): Future[Unit]

  def send(msg: ShutdownStore): Future[Unit]

  def send(msg: TransferStore): Future[Unit]
