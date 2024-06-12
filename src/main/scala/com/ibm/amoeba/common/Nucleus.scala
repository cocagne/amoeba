package com.ibm.amoeba.common

import java.util.UUID

import com.ibm.amoeba.common.objects.{Key, ObjectId, ObjectType}
import com.ibm.amoeba.common.pool.PoolId

object Nucleus {
  val objectId: ObjectId = ObjectId(new UUID(0, 0))

  val objectType: ObjectType.Value = ObjectType.KeyValue

  val poolId: PoolId = PoolId(new UUID(0, 0))

  private [amoeba] val PoolTreeKey = Key(Array[Byte](0))

  private [amoeba] val HostTreeKey = Key(Array[Byte](1))
}
