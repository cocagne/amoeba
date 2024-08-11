package org.aspen_ddp.aspen.fs.error

import org.aspen_ddp.aspen.fs.DirectoryPointer

case class DirectoryNotEmpty(pointer: DirectoryPointer) extends FSError
