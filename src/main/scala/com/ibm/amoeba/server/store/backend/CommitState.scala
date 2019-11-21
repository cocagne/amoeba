package com.ibm.amoeba.server.store.backend

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.store.StorePointer

case class CommitState(objectId: ObjectId,
                       storePointer: StorePointer,
                       metadata: Metadata,
                       objectType: ObjectType.Value,
                       data: DataBuffer,
                       maxSize: Option[Int])
