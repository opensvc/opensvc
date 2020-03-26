import exceptions as ex

from .. import adder as base_adder
from ..darwin import Fs
from utilities.proc import justcall

def adder(svc, s):
    base_adder(svc, s, drv=FsHfs)

class FsHfs(Fs):
   mkfs = ['newfs_hfs']
   info = ['diskutil', 'info']

   def do_mkfs(self):
       opts = self.oget("mkfs_opt")
       cmd = self.mkfs + opts + [self.mkfs_dev]
       ret, out, err = self.vcall(cmd)
       if ret != 0:
           self.log.error('Failed to format %s'%self.mkfs_dev)
           raise ex.Error
       self.start()
       cmd = ["diskutil", "enableOwnership", self.mkfs_dev]
       ret, out, err = self.vcall(cmd)
       if ret != 0:
           raise ex.Error

   def check_fs(self):
       """
          Device Identifier:        disk2
          Device Node:              /dev/disk2
          Whole:                    Yes
          Part of Whole:            disk2
          Device / Media Name:      Disk Image
       
          Volume Name:              Not applicable (no file system)
       
          Mounted:                  Not applicable (no file system)
       
          File System:              None
       
          Content (IOContent):      None
          OS Can Be Installed:      No
          Media Type:               Generic
          Protocol:                 Disk Image
          SMART Status:             Not Supported
       
          Total Size:               21.5 GB (21474836480 Bytes) (exactly 41943040 512-Byte-Units)
          Volume Free Space:        Not applicable (no file system)
          Device Block Size:        512 Bytes
       
          Read-Only Media:          No
          Read-Only Volume:         Not applicable (no file system)
       
          Device Location:          External
          Removable Media:          Yes
          Media Removal:            Software-Activated
       
          Virtual:                  Yes
          OS 9 Drivers:             No
          Low Level Format:         Not supported
       """
       cmd = self.info + [self.mkfs_dev]
       out, err, ret = justcall(cmd)
       if ret != 0:
           raise ex.Error(err)
       for line in out.splitlines():
           line = line.strip()
           if not line.startswith("File System:") and not line.startswith("File System Personality:"):
               continue
           if line.split()[-1] == "None":
               self.log.info("%s is not formatted", self.mkfs_dev)
               return False
           else:
               return True
       raise ex.Error("unable to determine if %s is formatted" % self.mkfs_dev)

