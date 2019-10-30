package com.ibm.amoeba.common.ida

import com.ibm.amoeba.AmoebaError

sealed abstract class IDAError extends AmoebaError

/** Thrown when an unknown IDA type is found embedded within a serialized ObjectPointer */
class IDAEncodingError extends IDAError

class IDARestoreError extends IDAError

class IDANotSupportedError extends IDAError
