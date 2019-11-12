package com.ibm.amoeba.common.store

object ReadError extends Enumeration {
  val StoreNotFound: Value = Value("StoreNotFound")
  val ObjectNotFound: Value = Value("ObjectNotFound")
}
