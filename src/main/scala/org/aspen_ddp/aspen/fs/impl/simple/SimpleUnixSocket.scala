package org.aspen_ddp.aspen.fs.impl.simple

import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.fs.{FileSystem, UnixSocket, UnixSocketInode, UnixSocketPointer}

class SimpleUnixSocket(override val pointer: UnixSocketPointer,
                       initialInode: UnixSocketInode,
                       revision: ObjectRevision,
                       fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with UnixSocket {

  override def inode: UnixSocketInode = super.inode.asInstanceOf[UnixSocketInode]
}
