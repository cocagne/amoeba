package org.aspen_ddp.aspen.fs.error

import org.aspen_ddp.aspen.fs.DirectoryPointer

case class DirectoryEntryDoesNotExist(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name does not exist in directory ${pointer.uuid}"
}