package com.ibm.amoeba.client

trait FinalizationActionFactory {
  def createFinalizationAction(client: AmoebaClient, data: Array[Byte]): FinalizationAction
}
