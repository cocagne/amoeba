package com.ibm.amoeba.server.cnc

import com.ibm.amoeba.client.Host

import scala.concurrent.Future


trait CnCFrontend:
  
  def host: Host
  
  def send(msg: NewStore): Future[Unit]

  def send(msg: ShutdownStore): Future[Unit]

  def send(msg: TransferStore): Future[Unit]
