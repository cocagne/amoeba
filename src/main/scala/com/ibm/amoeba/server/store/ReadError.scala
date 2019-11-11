package com.ibm.amoeba.server.store

object ReadError extends Enumeration {
  val StoreNotFound: Value = Value("StoreNotFound")
  val ObjectNotFound: Value = Value("ObjectNotFound")
}
