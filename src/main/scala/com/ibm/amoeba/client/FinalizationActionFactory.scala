package com.ibm.amoeba.client

import com.ibm.amoeba.common.transaction.TransactionDescription

trait FinalizationActionFactory {
  def createFinalizationAction(client: AmoebaClient, 
                               txd: TransactionDescription, 
                               data: Array[Byte]): FinalizationAction
}
