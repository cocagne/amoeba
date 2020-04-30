package com.ibm.amoeba.fs.error

case class InvalidInode(inodeNumber: Long) extends FSError
