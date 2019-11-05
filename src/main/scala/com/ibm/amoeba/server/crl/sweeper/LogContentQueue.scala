package com.ibm.amoeba.server.crl.sweeper

class LogContentQueue {
  var head: Option[LogContent] = None
  var tail: Option[LogContent] = None

  def add(item: LogContent): Unit = {
    item.prev = head
    item.next = None
    head.foreach { h => h.next = Some(item) }
    head = Some(item)
    if (tail.isEmpty)
      tail = Some(item)
  }

  def remove(item: LogContent): Unit = {
    tail.foreach { t =>
      if (t eq item)
        tail = item.next
    }
    head.foreach { h =>
      if (h eq item)
        head = item.prev
    }

    item.prev.foreach { p => p.next = item.next }
    item.next.foreach { n => n.prev = item.prev }

    item.prev = None
    item.next = None
  }

  def moveToHead(item: LogContent): Unit = {
    remove(item)
    add(item)
  }
}
