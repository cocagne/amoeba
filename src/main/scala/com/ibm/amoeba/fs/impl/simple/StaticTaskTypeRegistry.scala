package com.ibm.amoeba.fs.impl.simple

import java.util.UUID

import com.ibm.amoeba.compute.DurableTaskType

object StaticTaskTypeRegistry {
  val registeredTasks: Map[UUID, DurableTaskType] = Map(
    CreateFileTask.typeUUID -> CreateFileTask,
    UnlinkFileTask.typeUUID -> UnlinkFileTask
  )
}
