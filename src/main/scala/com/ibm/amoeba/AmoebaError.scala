package com.ibm.amoeba

abstract class AmoebaError(message:String = null, cause: Throwable = null) extends Exception(message, cause)
