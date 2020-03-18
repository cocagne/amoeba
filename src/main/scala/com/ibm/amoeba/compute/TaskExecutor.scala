package com.ibm.amoeba.compute

import com.ibm.amoeba.client.Transaction
import com.ibm.amoeba.common.objects.Key

import scala.concurrent.Future

trait TaskExecutor {
  def prepareTask(
                   taskType: DurableTaskType,
                   initialState: List[(Key, Array[Byte])])(implicit tx: Transaction): Future[Future[Option[AnyRef]]]

  def resume(): Unit
}
