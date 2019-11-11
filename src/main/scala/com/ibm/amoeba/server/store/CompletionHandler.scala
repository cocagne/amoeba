package com.ibm.amoeba.server.store

trait CompletionHandler {
  def complete(op: Completion)
}
