package com.ibm.amoeba.common.store

trait CompletionHandler {
  def complete(op: Completion)
}
