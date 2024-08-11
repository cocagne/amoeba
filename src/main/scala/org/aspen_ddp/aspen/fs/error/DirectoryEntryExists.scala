package org.aspen_ddp.aspen.fs.error

import org.aspen_ddp.aspen.fs.DirectoryPointer

case class DirectoryEntryExists(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name already exists in directory ${pointer.uuid}"
}
