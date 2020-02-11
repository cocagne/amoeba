package com.ibm.amoeba.client.internal

import java.util.UUID

import com.ibm.amoeba.client.RegisteredTypeFactory
import com.ibm.amoeba.client.tkvl.{KVObjectRootManager, SplitFinalizationAction}

object StaticTypeRegistry {

  private val registry: List[RegisteredTypeFactory] = List(
    KVObjectRootManager,
    SplitFinalizationAction
  )

  val types: List[(UUID, RegisteredTypeFactory)] = registry.map(t => t.typeUUID -> t)
}
