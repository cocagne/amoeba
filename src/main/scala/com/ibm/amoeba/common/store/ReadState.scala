package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectType}
import com.ibm.amoeba.common.transaction.TransactionId

case class ReadState(objectId: ObjectId,
                     metadata: Metadata,
                     objectType: ObjectType.Value,
                     data: DataBuffer,
                     lockedWriteTransactions: Set[TransactionId])
