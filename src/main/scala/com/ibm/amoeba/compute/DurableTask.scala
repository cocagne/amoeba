package com.ibm.amoeba.compute

import java.util.UUID

import com.ibm.amoeba.common.objects.{Key, ObjectRevision}

import scala.concurrent.Future

object DurableTask {
  val TaskTypeKey = Key(0xFF) // Corresponds to TaskType UUID

  val ReservedFromKeyId = 0xF0

  val IdleTaskType = new UUID(0,0)
  // Make a TaskGroup must be able to immediately start task (cause it returns a handle to result)
  // Could make one in terms of remote executor + tcp connection or via insert task and poll for
  // result
  // Ultimately need a UUID based tree similar to object allocators?
}

trait DurableTask {

  val taskPointer: DurableTaskPointer

  /** Future is to the revision of the object when the task completes plus an optional return value
    *
    *  The returned object revision is intended to facilitate re-use of existing Task objects by
    *  allowing the TaskGroupExecutor to learn the revision of the completed task without having to
    *  first read the state of the object
    */
  def completed: Future[(ObjectRevision, Option[AnyRef])]

  def resume(): Unit

  def suspend(): Unit
}
