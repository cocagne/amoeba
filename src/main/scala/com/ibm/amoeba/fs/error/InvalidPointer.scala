package com.ibm.amoeba.fs.error

/** Thrown when decoding a pointer that does not have an expected/supported type code*/
case class InvalidPointer(typeCode: Byte) extends FSError
