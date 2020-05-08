package com.ibm.amoeba.fs

trait UnixSocket extends BaseFile {
  val pointer: UnixSocketPointer
}
