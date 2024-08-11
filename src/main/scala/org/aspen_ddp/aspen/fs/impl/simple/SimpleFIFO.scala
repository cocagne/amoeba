package org.aspen_ddp.aspen.fs.impl.simple

import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.fs.{FIFO, FIFOInode, FIFOPointer, FileSystem}

class SimpleFIFO(override val pointer: FIFOPointer,
                 initialInode: FIFOInode,
                 revision: ObjectRevision,
                 fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with FIFO {

  override def inode: FIFOInode = super.inode.asInstanceOf[FIFOInode]
}
