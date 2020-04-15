import re

from .asset import BaseAsset
from utilities.proc import justcall

sim = False

class Asset(BaseAsset):
    def __init__(self, node):
        super(Asset, self).__init__(node)

    def convert(self, s, unit):
        if unit == "M":
            return int(float(s))
        elif unit == "G":
            return 1024 * int(float(s))
        else:
            return 0

    def _get_mem_bytes(self):
        # MB
        cmd = ['vmstat', '-P']
        out, err, ret = _justcall(cmd)
        for line in out.split('\n'):
            if 'Total' not in line:
                continue
            l = line.split()
            s = l[-2]
            unit = l[-1]
            mem = self.convert(s, unit)
            return str(mem)
        return '0'

    def _get_mem_banks(self):
        return '0'

    def _get_mem_slots(self):
        return '0'

    def _get_os_vendor(self):
        return 'HP'

    def _get_os_release(self):
        cmd = ['uname', '-a']
        out, err, ret = _justcall(cmd)
        l = out.split()
        return ' '.join(l[2:4])

    def _get_os_kernel(self):
        from distutils.version import LooseVersion as V # pylint: disable=no-name-in-module,import-error
        cmd = ['dupatch', '-track', '-type', 'kit', '-nolog']
        out, err, ret = _justcall(cmd)
        l = []
        for line in out.split('\n'):
            line = line.strip()
            if not line.startswith('- T64') or not 'OSF' in line:
                continue
            l.append(line.split()[1])
        if len(l) == 0:
            return 'Unknown'
        l.sort(lambda x, y: V(x) < V(y))
        return l[-1].split('-')[0]

    def _get_os_arch(self):
        cmd = ['uname', '-a']
        out, err, ret = _justcall(cmd)
        l = out.split()
        return l[-1]

    def _get_cpu_freq(self):
        cmd = ['psrinfo', '-v']
        out, err, ret = _justcall(cmd)
        for line in out.split('\n'):
            if 'operates at' not in line:
                continue
            l = line.split()
            if len(l) < 2:
                continue
            return l[-2]
        return '0'

    def _get_cpu_cores(self):
        return self._get_cpu_dies()

    def _get_cpu_dies(self):
        cmd = ['psrinfo']
        out, err, ret = _justcall(cmd)
        return str(len(out.split('\n'))-1)

    def _get_cpu_model(self):
        cmd = ['psrinfo', '-v']
        out, err, ret = _justcall(cmd)
        for line in out.split('\n'):
            if 'operates at' not in line:
                continue
            l = line.split()
            if len(l) < 3:
                continue
            return l[2]
        return 'Unknown'

    def _get_serial(self):
        cmd = ['consvar', '-g', 'sys_serial_num']
        out, err, ret = _justcall(cmd)
        l = out.split('=')
        if len(l) == 2:
            return l[1].strip()
        return 'Unknown'

    def _get_model(self):
        cmd = ["hwmgr", "-v", "h"]
        out, err, ret = _justcall(cmd)
        for line in out.split('\n'):
            if "platform" not in line:
                continue
            l = line.split("platform")
            if len(l) != 2:
                continue
            return l[1].strip()
        return 'Unknown'

    def is_id(self, line):
        if re.match(r"^\W*[0-9]*:", line) is None:
            return False
        return True

    def __get_hba(self):
        # fc / fcoe
        cmd = ['hwmgr', '-show', 'fibre', '-ada']
        out, err, ret = _justcall(cmd)
        hba = {}
        for line in out.split('\n'):
            if self.is_id(line):
                l = line.split()
                hba_name = l[1]
            elif 'WWPN' in line:
                l = line.split()
                hba_portname = l[1].replace('-', '').lower()
                hba[hba_name] = (hba_portname, 'fc')
        return hba

    def _get_hba(self):
        hba = self.__get_hba()
        return hba.values()

    def _get_targets(self):
        # fc / fcoe
        cmd = ['hwmgr', '-show', 'fibre', '-topo']
        out, err, ret = _justcall(cmd)
        tgt = []
        hba = self.__get_hba()
        for line in out.split('\n'):
            if self.is_id(line):
                l = line.split()
                hba_name = l[1]
            elif line.strip().startswith('0x'):
                l = line.split()
                if l[1].startswith('-'):
                    continue
                tgt_portname = l[2].replace('-', '').lower()
                hba_portname = hba[hba_name][0]
                tgt.append((hba_portname, tgt_portname))
        return tgt

def _justcall(cmd):
    if not sim:
        return justcall(cmd)

    data = {}

    data[('hwmgr', '-show', 'fibre', '-ada')] = """

            ADAPTER   LINK    LINK             FABRIC     SCSI     CARD
     HWID:  NAME      STATE   TYPE             STATE      BUS      MODEL
    --------------------------------------------------------------------------------
       53:  emx0      up      point-to-point   attached   scsi3    KGPSA-CA

    		Revisions:  driver 2.17           firmware 3.93A0
    		FC Address: 0x1ece00
    		TARGET:     -1
    		WWPN/WWNN:  1000-0000-c922-585c   2000-0000-c922-585c

            ADAPTER   LINK    LINK             FABRIC     SCSI     CARD
     HWID:  NAME      STATE   TYPE             STATE      BUS      MODEL
    --------------------------------------------------------------------------------
       61:  emx1      up      point-to-point   attached   scsi4    KGPSA-CA

    		Revisions:  driver 2.17           firmware 3.93A0
    		FC Address: 0x1cce00
    		TARGET:     -1
    		WWPN/WWNN:  1000-0000-c924-a43d   2000-0000-c924-a43d

    """

    data[('hwmgr', '-show', 'fibre', '-topo')] = """

            ADAPTER   LINK    LINK             FABRIC     SCSI     CARD
     HWID:  NAME      STATE   TYPE             STATE      BUS      MODEL
    --------------------------------------------------------------------------------
       53:  emx0      up      point-to-point   attached   scsi3    KGPSA-CA

    	FC DID	  TARGET    WWPN     		 WWNN                 lfd  LSIT
    	------------------------------------------------------------------------
    	0x382200      2     5000-1fe1-5012-9d49  5000-1fe1-5012-9d40  l--  L--T
    	0x381200      3     5000-1fe1-5012-9d4f  5000-1fe1-5012-9d40  l--  L--T
    	0x380200      0     5000-1fe1-5012-9d4d  5000-1fe1-5012-9d40  l--  L--T
    	0x383200      1     5000-1fe1-5012-9d4b  5000-1fe1-5012-9d40  l--  L--T
    	0xfffffc     -1     21fc-0005-1e36-2110  1000-0005-1e36-2110  l-d  ----
    	0xfffffe     -1     20ce-0005-1e36-2110  1000-0005-1e36-2110  lf-  ----

            ADAPTER   LINK    LINK             FABRIC     SCSI     CARD
     HWID:  NAME      STATE   TYPE             STATE      BUS      MODEL
    --------------------------------------------------------------------------------
       61:  emx1      up      point-to-point   attached   scsi4    KGPSA-CA

    	FC DID	  TARGET    WWPN     		 WWNN                 lfd  LSIT
    	------------------------------------------------------------------------
    	0xef1200      2     5000-1fe1-5012-9d4e  5000-1fe1-5012-9d40  l--  L--T
    	0xef3200      0     5000-1fe1-5012-9d4a  5000-1fe1-5012-9d40  l--  L--T
    	0xef0200      3     5000-1fe1-5012-9d4c  5000-1fe1-5012-9d40  l--  L--T
    	0xef2200      1     5000-1fe1-5012-9d48  5000-1fe1-5012-9d40  l--  L--T
    	0xfffffc     -1     21fc-0005-1e36-1eee  1000-0005-1e36-1eee  l-d  ----
    	0xfffffe     -1     20ce-0005-1e36-1eee  1000-0005-1e36-1eee  lf-  ----

    """

    data[('hwmgr', '-show', 'scsi', '-full', '-id', '83', '-nowrap')] = """

            SCSI                DEVICE    DEVICE  DRIVER NUM  DEVICE FIRST
     HWID:  DEVICEID HOSTNAME   TYPE      SUBTYPE OWNER  PATH FILE   VALID PATH
    -------------------------------------------------------------------------
       83:  16       wrus01     disk      none    2      8    dsk13  [3/2/1]

          WWID:01000010:6005-08b4-000b-440a-0000-f000-1325-0000


          BUS   TARGET  LUN   PATH STATE
          ---------------------------------
          3     2       1     valid
          3     3       1     valid
          3     1       1     valid
          3     0       1     valid
          4     3       1     valid
          4     2       1     valid
          4     1       1     valid
          4     0       1     valid

    """

    data[('uname', '-a')] = """OSF1 wrus01 V5.1 2650 alpha
    """

    data[('dupatch', '-track', '-type', 'kit', '-nolog')] = """
    Gathering details of relevant patches, this may take a bit of time


    	Patches installed on the system came from following software kits:
    	------------------------------------------------------------------

    	- T64V51BB24AS0003-20030929 OSF540
    	- T64V51BB26AS0005-20050502 IOS540
    	- T64V51BB26AS0005-20050502 OSF540
              ================
                 kernelver

    				NOTE

    	When a patch kit is listed, it does not necessarily mean
    	all patches on that kit are installed on your system.
    """

    data[('psrinfo',)] = """0	on-line   since 05/19/2012 16:00:50
    """

    data[('psrinfo', '-v')] = """Status of processor 0 as of: 06/15/12 18:35:40
      Processor has been on-line since 05/19/2012 16:00:50
      The alpha EV6.8CB (21264C) processor operates at 1000 MHz,
      has a cache size of 8388608 bytes,
      and has an alpha internal floating point processor.
    """

    data[('consvar', '-g', 'sys_serial_num')] = """ sys_serial_num = AY14610125
    """

    data[('hwmgr', '-v', 'h')] = """HWID:   hardware hierarchy
    -------------------------------------------------------------------------------
       1:   platform AlphaServer ES45 Model 2
       2:     cpu CPU0
       6:     bus iop0
       7:       bus hose0
    """

    data[('ifconfig', '-a')] = """ee0: flags=c63<UP,BROADCAST,NOTRAILERS,RUNNING,MULTICAST,SIMPLEX>
         inet 10.6.65.37 netmask fffffc00 broadcast 10.6.67.255 ipmtu 1500
         inet 10.6.66.160 netmask fffffc00 broadcast 10.6.67.255 ipmtu 1500

    ee1: flags=c63<UP,BROADCAST,NOTRAILERS,RUNNING,MULTICAST,SIMPLEX>
         inet 10.40.32.241 netmask fffffc00 broadcast 10.40.35.255 ipmtu 1500

    lo0: flags=100c89<UP,LOOPBACK,NOARP,MULTICAST,SIMPLEX,NOCHECKSUM>
         inet 127.0.0.1 netmask ff000000 ipmtu 4096

    sl0: flags=10<POINTOPOINT>

    tun0: flags=80<NOARP>

    tun1: flags=80<NOARP>
    """

    data[('vmstat', '-P')] = """Total Physical Memory =  1024.00 M
    """

    return data[tuple(cmd)], '', 0


if __name__ == "__main__":
    o = Asset("wrus01")
    print(o._get_mem_bytes())
    print(o._get_os_release())
    print(o._get_os_kernel())
    print(o._get_os_arch())
    print(o._get_cpu_freq())
    print(o._get_cpu_cores())
    print(o._get_cpu_dies())
    print(o._get_cpu_model())
    print(o._get_serial())
    print(o._get_model())
    print(o._get_hba())
    print(o._get_targets())
