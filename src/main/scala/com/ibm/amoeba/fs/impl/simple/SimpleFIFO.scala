package com.ibm.amoeba.fs.impl.simple

import com.ibm.amoeba.common.objects.ObjectRevision
import com.ibm.amoeba.fs.{FIFO, FIFOInode, FIFOPointer, FileSystem}

class SimpleFIFO(override val pointer: FIFOPointer,
                 initialInode: FIFOInode,
                 revision: ObjectRevision,
                 fs: FileSystem) extends SimpleBaseFile(pointer, revision, initialInode, fs) with FIFO {

  override def inode: FIFOInode = super.inode.asInstanceOf[FIFOInode]
}
