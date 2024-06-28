package com.ibm.amoeba.server.store.backend

sealed abstract class BackendType()

case class RocksDBType() extends BackendType
