package com.ibm.amoeba.fs.error

import com.ibm.amoeba.fs.DirectoryPointer

case class DirectoryNotEmpty(pointer: DirectoryPointer) extends FSError
