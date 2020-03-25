package com.ibm.amoeba.common.objects

import com.ibm.amoeba.AmoebaError

class KeyAlreadyExists(val key: Key) extends AmoebaError
