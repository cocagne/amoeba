package com.ibm.amoeba.fs

import com.ibm.amoeba.client.AmoebaClient

import scala.concurrent.ExecutionContext

trait FileSystem {
  private[fs] def client: AmoebaClient
  private[fs] def executionContext: ExecutionContext
}
