package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.AmoebaError

class CorruptedEntry(msg: String) extends AmoebaError(msg)
