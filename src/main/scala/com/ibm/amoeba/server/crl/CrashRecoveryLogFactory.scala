package com.ibm.amoeba.server.crl

trait CrashRecoveryLogFactory {
  def createCRL(): CrashRecoveryLog
}
