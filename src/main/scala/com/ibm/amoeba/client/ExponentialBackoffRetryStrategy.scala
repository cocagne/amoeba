package com.ibm.amoeba.client

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class ExponentialBackoffRetryStrategy(client: AmoebaClient, backoffLimit: Int = 60 * 1000, initialRetryDelay: Int = 15) extends RetryStrategy {

  private [this] var exit = false

  def shutdown(): Unit = synchronized { exit = true }

  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] = {
    val p = Promise[T]()

    implicit val ec: ExecutionContext = client.clientContext

    def retry(limit: Int): Unit = {
      val shouldAttempt = synchronized { !exit }

      if (shouldAttempt) {

        def scheduleNextAttempt(): Unit = {
          val rand = new java.util.Random
          val delay = rand.nextInt(limit)
          val nextLimit = if (limit * limit < backoffLimit) limit * limit else backoffLimit
          client.backgroundTasks.schedule(Duration(delay, TimeUnit.MILLISECONDS))(retry(nextLimit))
        }

        try {
          attempt onComplete {
            case Success(result) => p.success(result)

            case Failure(cause) => cause match {
              case StopRetrying(reason) => p.failure(reason)
              case _: Throwable => scheduleNextAttempt()
            }
          }
        } catch {
          case StopRetrying(reason) => p.failure(reason)
          case _: Throwable => scheduleNextAttempt()
        }
      }
    }

    retry(initialRetryDelay)

    p.future
  }

  def retryUntilSuccessful[T](onAttemptFailure: Throwable => Future[Unit])(attempt: => Future[T]): Future[T] = {
    val p = Promise[T]()

    implicit val ec: ExecutionContext = client.clientContext

    def retry(limit: Int): Unit = {
      val shouldAttempt = synchronized { !exit }

      if (shouldAttempt) {

        def scheduleNextAttempt(cause: Throwable): Unit = {
          val rand = new java.util.Random
          val delay = rand.nextInt(limit)
          val nextLimit = if (limit * limit < backoffLimit) limit * limit else backoffLimit

          client.backgroundTasks.schedule(Duration(delay, TimeUnit.MILLISECONDS)) {
            retryUntilSuccessful(onAttemptFailure(cause)).onComplete {
              case Failure(reason) => p.failure(reason)
              case Success(_) => retry(nextLimit)
            }
          }
        }

        try {
          attempt onComplete {
            case Success(result) => p.success(result)

            case Failure(cause) =>
              cause match {
                case StopRetrying(reason) => p.failure(reason)
                case cause: Throwable => scheduleNextAttempt(cause)
              }
          }
        } catch {
          case StopRetrying(reason) => p.failure(reason)
          case cause: Throwable => scheduleNextAttempt(cause)
        }
      }
    }

    retry(initialRetryDelay)

    p.future
  }
}
