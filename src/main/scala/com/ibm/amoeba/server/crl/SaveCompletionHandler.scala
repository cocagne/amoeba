package com.ibm.amoeba.server.crl

trait SaveCompletionHandler {
  def saveComplete(op: SaveCompletion): Unit
}
