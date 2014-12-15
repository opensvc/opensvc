import rcSysReport

class SysReport(rcSysReport.SysReport):
    def __init__(self, node=None):
        rcSysReport.SysReport.__init__(self, node=node)
        self.todo += [
          ('FILE', '/etc/resolv.conf'),
          ('FILE', '/etc/nsswitch.conf'),
          ('CMD', '/sbin/dmsetup ls'),
          ('CMD', '/sbin/dmsetup table'),
          ('CMD', '/sbin/fdisk -l'),
          ('FILE', '/opt/opensvc/etc/node.conf'),
          ('GLOB', '/opt/opensvc/etc/*env'),
          ('DIR', '/opt/opensvc/etc/sysreport.conf.d'),
        ]

