package com.ibm.amoeba.server.crl.simple

import java.nio.ByteBuffer

abstract class StreamReader:
  def read(location: StreamLocation): ByteBuffer
