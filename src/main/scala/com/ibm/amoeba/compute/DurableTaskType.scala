package com.ibm.amoeba.compute

import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, KeyValueObjectState}
import com.ibm.amoeba.common.objects.{Key, ObjectRevision}


trait DurableTaskType {

  val typeUUID: UUID

  def createTask(client: AmoebaClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask

}
