package com.ibm.amoeba.fs.error

import com.ibm.amoeba.fs.DirectoryPointer

case class DirectoryEntryExists(pointer: DirectoryPointer, name: String) extends FSError {
  override def toString: String = s"$name already exists in directory ${pointer.uuid}"
}
