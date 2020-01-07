package com.ibm.amoeba.common

import java.util.UUID

import com.ibm.amoeba.common.objects.{ObjectId, ObjectType}
import com.ibm.amoeba.common.pool.PoolId

object Nucleus {
  val objectId: ObjectId = ObjectId(new UUID(0, 0))

  val objectType: ObjectType.Value = ObjectType.KeyValue

  val poolId: PoolId = PoolId(new UUID(0, 0))
}
