package com.ibm.amoeba.common.objects

object ReadError extends Enumeration {
  val ObjectMismatch: Value = Value
  val ObjectNotFound: Value = Value
  val StoreNotFound: Value = Value
  val CorruptedObject: Value = Value

}
