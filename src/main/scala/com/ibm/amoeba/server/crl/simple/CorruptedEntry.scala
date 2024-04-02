package com.ibm.amoeba.server.crl.simple

import com.ibm.amoeba.AmoebaError

class CorruptedEntry(msg: String) extends AmoebaError(msg)