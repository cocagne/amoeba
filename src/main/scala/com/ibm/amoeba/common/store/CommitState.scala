package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}

case class CommitState(objectId: ObjectId,
                       storePointer: StorePointer,
                       metadata: Metadata,
                       objectType: ObjectType.Value,
                       data: DataBuffer)
