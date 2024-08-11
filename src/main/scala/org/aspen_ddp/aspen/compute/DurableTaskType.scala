package org.aspen_ddp.aspen.compute

import java.util.UUID

import org.aspen_ddp.aspen.client.{AmoebaClient, KeyValueObjectState}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}


trait DurableTaskType {

  val typeUUID: UUID

  def createTask(client: AmoebaClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask

}
