package com.ibm.amoeba.common.transaction

import java.util.UUID

import com.ibm.amoeba.common.DataBuffer

case class ObjectUpdate(objectUUID: UUID, data: DataBuffer)
