package com.ibm.amoeba.common.transaction

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.ObjectId

case class ObjectUpdate(objectId: ObjectId, data: DataBuffer)
