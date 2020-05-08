package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.common.objects.ObjectRevision
import com.ibm.amoeba.fs.{FileSystem, UnixSocket, UnixSocketInode, UnixSocketPointer}

class SimpleUnixSocket(override val pointer: UnixSocketPointer,
                       initialInode: UnixSocketInode,
                       revision: ObjectRevision,
                       fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with UnixSocket {

  override def inode: UnixSocketInode = super.inode.asInstanceOf[UnixSocketInode]
}
