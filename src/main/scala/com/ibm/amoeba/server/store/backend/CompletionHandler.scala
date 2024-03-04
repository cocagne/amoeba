package com.ibm.amoeba.server.store.backend

trait CompletionHandler {
  def complete(op: Completion): Unit
}
