package com.ibm.amoeba.client

import com.ibm.amoeba.AmoebaError

case class StopRetrying(reason: Throwable) extends AmoebaError("StopRetrying", reason)
