package com.ibm.amoeba.common.objects

import scala.language.implicitConversions

final case class Value(bytes: Array[Byte]) extends AnyVal
