package com.ibm.amoeba.fs.impl

import java.util.UUID

import com.ibm.amoeba.client.RegisteredTypeFactory
import com.ibm.amoeba.fs.impl.simple.SimpleDirectoryRootManager

object StaticTypeRegistry {

  private val registry: List[RegisteredTypeFactory] = List(
   SimpleDirectoryRootManager
  )

  val types: List[(UUID, RegisteredTypeFactory)] = registry.map(t => t.typeUUID -> t)
}
