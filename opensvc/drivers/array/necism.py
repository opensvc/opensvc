import core.exceptions as ex
from utilities.proc import justcall, which

class NecMixin(object):
    """
    BV OS Types
    ===========

    A2 the logical disk is operating on an ACOS-2 system.
    A4 the logical disk is operating on an ACOS-4 system.
    AX the logical disk is operating on an AIX system.
    CX the logical disk is operating on a Solaris system.
    LX the logical disk is operating on a Linux system.
    NX the logical disk is operating on an HP-UX system.
    SX the logical disk is operating on a SUPER-UX system.
    WN the logical disk is operating on a Windows system (excluding GPT disk).
    WG the logical disk is operating on a Windows system (GPT disk).

    """
    arrays = []
    view_bin = None
    sc_query_bin = None
    sc_linkinfo_bin = None
    sc_unlink_bin = None
    sc_link_bin = None
    sc_create_bin = None
    bv_os_types = ["-", "A2", "A4", "AX", "LX", "NX", "SX", "WN", "WG"]

    def get_bin(self, bin_attr, candidates):
        if getattr(self, bin_attr) is not None:
            return
        for bin in candidates:
            if which(bin) is not None:
                setattr(self, bin_attr, bin)
                break
        if getattr(self, bin_attr) is None:
            raise ex.Error('Can not find %s program in PATH' % ' or '.join(candidates))

    def get_view_bin(self):
        self.get_bin('view_bin', ['iSMcc_view', 'iSMview'])

    def get_sc_query_bin(self):
        self.get_bin('sc_query_bin', ['iSMsc_query'])

    def get_sc_linkinfo_bin(self):
        self.get_bin('sc_linkinfo_bin', ['iSMsc_linkinfo'])

    def get_sc_link_bin(self):
        self.get_bin('sc_link_bin', ['iSMsc_link'])

    def get_sc_create_bin(self):
        self.get_bin('sc_create_bin', ['iSMsc_create'])

    def get_sc_unlink_bin(self):
        self.get_bin('sc_unlink_bin', ['iSMsc_unlink'])

    def view_cmd(self, cmd, on_array=True):
        self.get_view_bin()
        cmd = [self.view_bin] + cmd
        if on_array:
            cmd += [self.name]
        return justcall(cmd)

    def sc_query_cmd(self, cmd):
        self.get_sc_query_bin()
        cmd = [self.sc_query_bin] + cmd
        return justcall(cmd)

    def sc_linkinfo_cmd(self, cmd):
        self.get_sc_linkinfo_bin()
        cmd = [self.sc_linkinfo_bin] + cmd
        return justcall(cmd)

    def sc_unlink_cmd(self, cmd):
        self.get_sc_unlink_bin()
        cmd = [self.sc_unlink_bin] + cmd
        self.log.info(' '.join(cmd))
        return justcall(cmd)

    def sc_link_cmd(self, cmd):
        self.get_sc_link_bin()
        cmd = [self.sc_link_bin] + cmd
        self.log.info(' '.join(cmd))
        return justcall(cmd)

    def sc_create_cmd(self, cmd):
        self.get_sc_create_bin()
        cmd = [self.sc_create_bin] + cmd
        self.log.info(' '.join(cmd))
        return justcall(cmd)

    def sc_create_ld(self, bv, sv):
        cmd = ['-bv', bv, '-sv', sv, '-bvflg', 'ld', '-svflg', 'ld']
        out, err, ret = self.sc_create_cmd(cmd)
        self.log.info(out)
        if ret != 0:
            raise ex.Error(err)

    def sc_unlink_ld(self, ld):
        cmd = ['-lv', ld, '-lvflg', 'ld']
        out, err, ret = self.sc_unlink_cmd(cmd)
        self.log.info(out)
        if ret != 0:
            raise ex.Error(err)

    def sc_link_ld(self, sv, ld):
        cmd = ['-lv', ld, '-sv', sv, '-lvflg', 'ld', '-svflg', 'ld']
        out, err, ret = self.sc_link_cmd(cmd)
        self.log.info(out)
        if ret != 0:
            raise ex.Error(err)

    def get_arrays(self):
        cmd = ['-d']
        out, err, ret = self.view_cmd(cmd, on_array=False)
        if ret != 0:
            self.refresh_vollist()
        out, err, ret = self.view_cmd(cmd, on_array=False)
        if ret != 0:
            raise ex.Error(err)

        """

--- Disk Array List ---
Product ID        Disk Array Name                   Resource State  Monitoring
D1-10             D1_10                             ready           running


--- Disk Array List ---
Product ID        Disk Array Name                   Resource State
Optima3600        Optima7_LMW                       ready


        """
        lines = out.split('\n')
        for line in lines:
            if len(line) == 0:
                continue
            if '---' in line:
                continue
            if 'Product ID' in line:
                continue
            l = line.split()
            if len(l) < 3:
                continue
            if self.filtering and l[1] not in self.objects:
                continue
            self.arrays.append(NecIsm(l[1]))

    def sc_linkinfo_ld(self, vol):
        """

Specified Volume Information
SV:LD Name      : test_src_0000_SV0014
   Type         : LX
   Special File : -
   State        : link   (test_src_0000_LV0064)
   Mode         : nr

Destination Volume Information
    LV:test_src_0000_LV0050     LX link   (test_src_0000_SV0016)    rw
    LV:test_src_0000_LV005A     LX link   (test_src_0000_SV0015)    rw
    LV:test_src_0000_LV0064     LX link   (test_src_0000_SV0014)    rw

"""
        cmd = ['-vol', vol, '-volflg', 'ld', '-lcl']
        out, err, ret = self.sc_linkinfo_cmd(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = {'dst': []}
        for line in out.split('\n'):
            if line.startswith('SV:LD Name'):
                data['SV:LD Name'] = line.split(': ')[1]
            elif line.strip().startswith('Type'):
                data['Type'] = line.split(': ')[1]
            elif line.strip().startswith('Special File'):
                data['Special File'] = line.split(': ')[1]
            elif line.strip().startswith('State'):
                data['State'] = line.split(': ')[1]
            elif line.strip().startswith('Mode'):
                data['Mode'] = line.split(': ')[1]
            elif line.strip().startswith('LV:'):
                data['dst'].append(line.split(':')[1])
        return data

    def sc_query_ld(self, sv):
        """
BV Information
    LD Name      : test_src_0000
    Type         : LX
    Special File : /dev/sdc
    State        : normal
    Reserve Area : -

SV Information
  LX:test_src_0000_SV0014    ( -1) snap/active   [2014/03/24 11:16:16] link
        """
        cmd = ['-sv', sv, '-svflg', 'ld']
        out, err, ret = self.sc_query_cmd(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = {'sv': []}
        for line in out.splitlines():
            line = line.strip()
            if line.startswith('LD Name'):
                data['LD Name'] = line.split(': ')[1]
            elif line.startswith('Type'):
                data['Type'] = line.split(': ')[1]
            elif line.startswith('Special File'):
                data['Special File'] = line.split(': ')[1]
            elif line.startswith('State'):
                data['State'] = line.split(': ')[1]
            elif line.startswith('Reserve Area'):
                data['Reserve Area'] = line.split(': ')[1]
            elif line.split(":", 1)[0] in self.bv_os_types:
                data['sv'].append(line[line.index(':')+1:])
        return data

    def sc_query_bv_detail(self, bv):
        """
        BV Information
            LD Name      : xxxxxxxxxxx_00CC
            Type         : LX
            Special File : /dev/sdaq
            State        : normal
            Reserve Area : -

        Pair Information
            SV:LD Name              : xxxxxxxxxxx_00cc_SV00ce
               Type                 : LX
               Generation(Attribute): -1(normal)
               Snap State           : snap/active        [2014/09/09 17:27:45]
               Create   Start Time  : 2014/09/09 17:27:45
               Processing Data Size : -
               Snapshot Data Size   : 47.4GB
               SV Guard             : off
               LV Link Status       : link
            LV:LD Name              : xxxxxxxxxxx_00cc_LV00cf
               Type                 : LX
               Special File         : /dev/sdar
               LV Access            : rw
            SV:LD Name              : xxxxxxxxxxx_00cc_SV00cd
               Type                 : LX
               Generation(Attribute): -2(normal)
               Snap State           : snap/active        [2014/09/09 17:19:12]
               Create   Start Time  : 2014/09/09 17:19:12
               Processing Data Size : -
               Snapshot Data Size   : 11.6GB
               SV Guard             : off
               LV Link Status       : link
            LV:LD Name              : xxxxxxxxxxx_00cc_LV00d0
               Type                 : LX
               LV Access            : rw
        """
        cmd = ['-bv', bv, '-bvflg', 'ld', '-detail']
        out, err, ret = self.sc_query_cmd(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = {
          'sv': {},
          'lv': {}
        }
        section = ""
        for line in out.split('\n'):
            line = line.strip()
            if line.startswith("BV Information"):
                section = "bvinfo"
                continue
            elif line.startswith("Pair Information"):
                section = "pairinfo"
                continue

            if section == "bvinfo" and line.startswith("LD Name"):
                data['LD Name'] = line.split(': ')[1]
            elif section == "bvinfo" and line.startswith("State"):
                data['State'] = line.split(': ')[1]

            elif section.startswith("pairinfo") and line.startswith("SV:LD Name"):
                ld_name = line.split(': ')[1]
                if ld_name not in data['sv']:
                    data['sv'][ld_name] = {}
                section = "pairinfo_sv"
            elif section == "pairinfo_sv" and line.startswith("Snap State"):
                data['sv'][ld_name]["Snap State"] = line.split(': ')[1]


            elif section.startswith("pairinfo") and line.startswith("LV:LD Name"):
                ld_name = line.split(': ')[1]
                if ld_name not in data['lv']:
                    data['lv'][ld_name] = {}
                section = "pairinfo_lv"
            elif section == "pairinfo_lv" and line.startswith("Type"):
                data['lv'][ld_name]["Type"] = line.split(': ')[1]

        return data



class NecIsms(NecMixin):
    def __init__(self, objects=None):
        if objects is None:
            objects = []
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.get_arrays()

    def __iter__(self):
        for array in self.arrays:
            yield(array)

    def refresh_vollist(self):
        if which('iSMvollist') is None:
            return
        cmd = ['iSMvollist', '-r']
        out, err, ret = justcall(cmd)

class NecIsm(NecMixin):
    def __init__(self, name):
        self.keys = ['all']
        self.name = name

    def get_all(self):
        cmd = ['-all']
        out, err, ret = self.view_cmd(cmd)
        return out

if __name__ == "__main__":
    o = NecIsms()
    for necism in o:
        print(necism.get_all())
        #print(o.sc_linkinfo_ld("test_src_0000_SV0014"))
        #print(o.sc_query_ld("test_src_0000_SV0014"))

