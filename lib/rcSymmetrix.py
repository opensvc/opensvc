from __future__ import print_function

import sys
import os
import ConfigParser
import json
from xml.etree.ElementTree import XML, fromstring

import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import justcall, which, convert_size
from rcOptParser import OptParser
from optparse import Option

PROG = "nodemgr array"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", action="store", dest="array_name",
        help="The name of the array, as defined in auth.conf"),
    "name": Option(
        "--name", action="store", dest="dev",
        help="The device identifier name (ex: mysvc_1)"),
    "dev": Option(
        "--dev", action="store", dest="dev",
        help="The device id (ex: 0A04)"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --targetgroup and --initiatorgroup. Can be specified multiple times."),
    "pools": Option(
        "--pools", action="append", dest="pools",
        help="A pool hosting the TDEV. Multiple --pools can be set."),
    "size": Option(
        "--size", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Add actions": {
        "add_disk": {
            "msg": "Add and present a thin device.",
            "options": [
                OPT.name,
                OPT.size,
                OPT.mappings,
                OPT.pools,
            ],
        },
        "add_tdev": {
            "msg": "Add a thin device. No pool bindings, no masking.",
            "options": [
                OPT.name,
                OPT.size,
            ],
        },
    },
    "Delete actions": {
        "del_disk": {
            "msg": "Unpresent and delete a thin device.",
            "options": [
                OPT.dev,
            ],
        },
        "del_tdev": {
            "msg": "Delete a thin device. No unmasking.",
            "options": [
                OPT.dev,
            ],
        },
    },
    "Modify actions": {
        "resize_tdev": {
            "msg": "Resize a thin device.",
            "options": [
                OPT.dev,
                OPT.size,
            ],
        },
    },
    "List actions": {
        "list_pools": {
            "msg": "List device pools.",
        },
        "list_tdevs": {
            "msg": "List thin devices.",
        },
        "list_views": {
            "msg": "List views, eg. groups of initiators/targets/devices.",
        },
    },
}


class Arrays(object):
    arrays = []

    def find_symcli_path(self):
        symcli_bin = which("symcli")
        if symcli_bin is not None:
            return os.path.dirname(symcli_bin)
        symcli_bin = "/usr/symcli/bin/symcli"
        if os.path.exists(symcli_bin):
            return os.path.dirname(symcli_bin)
        symcli_bin = "/opt/emc/SYMCLI/bin/symcli"
        if os.path.exists(symcli_bin):
            return os.path.dirname(symcli_bin)

    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        cf = rcEnv.authconf
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)

        self.index = 0
        self.symms = []
        for s in conf.sections():
            if self.filtering and s not in self.objects:
                continue

            try:
                stype = conf.get(s, 'type')
            except:
                continue
            if stype != "symmetrix":
                continue
            try:
                name = s
            except:
                print("error parsing section", s, file=sys.stderr)
                continue

            if conf.has_option(s, 'symcli_path'):
                symcli_path = conf.get(s, 'symcli_path')
            else:
                symcli_path = find_symcli_path()

            if symcli_path is None:
                print("symcli path not found for array", s, file=sys.stderr)
                continue

            if conf.has_option(s, 'symcli_connect'):
                symcli_connect = conf.get(s, 'symcli_connect')
		os.environ["SYMCLI_CONNECT"] = symcli_connect
            else:
                symcli_connect = None

            if conf.has_option(s, 'username'):
                username = conf.get(s, 'username')
            else:
                username = None

            if conf.has_option(s, 'password'):
                password = conf.get(s, 'password')
            else:
                password = None

            symcfg = os.path.join(symcli_path, "symcfg")
            if which(symcfg) is None:
                raise ex.excError('can not find symcfg in %s' % symcli_path)

            out, err, ret = justcall([symcfg, 'list', '-sid', name, '-output', 'xml_element'])
            if ret != 0:
                print(err, file=sys.stderr)
                continue
            tree = fromstring(out)
            for symm in tree.getiterator('Symm_Info'):
                model = symm.find('model').text
                if model.startswith('VMAX'):
                    self.arrays.append(Vmax(name, symcli_path, symcli_connect, username, password))
                elif 'DMX' in model or '3000-M' in model:
                    self.arrays.append(Dmx(name, symcli_path, symcli_connect, username, password))
                else:
                    print("unsupported sym model: %s" % model, file=sys.stderr)

        del(conf)

    def get_array(self, name):
        for array in self.arrays:
            if array.sid == name:
                return array

    def __iter__(self):
        for array in self.arrays:
            yield(array)


class Sym(object):
    def __init__(self, sid, symcli_path, symcli_connect, username, password):
        self.keys = ['sym_info',
                     'sym_dir_info',
                     'sym_dev_info',
                     'sym_dev_wwn_info',
                     'sym_dev_name_info',
                     'sym_devrdfa_info',
                     'sym_ficondev_info',
                     'sym_meta_info',
                     'sym_disk_info',
                     'sym_diskgroup_info',
                     'sym_pool_info',
                     'sym_tdev_info',
                     'sym_srp_info',
                     'sym_slo_info']
        self.sid = sid
        self.symcli_path = symcli_path
        self.symcli_connect = symcli_connect
        self.username = username
        self.password = password

    def set_environ(self):
        if self.symcli_connect:
            os.environ["SYMCLI_CONNECT"] = self.symcli_connect
	elif "SYMCLI_CONNECT" in os.environ:
            del os.environ["SYMCLI_CONNECT"]

    def symcmd(self, cmd, xml=True):
        self.set_environ()
        cmd += ['-sid', self.sid]
        if xml:
            cmd += ['-output', 'xml_element']
        return justcall(cmd)

    def symcfg(self, cmd, xml=True):
        cmd = ['/usr/symcli/bin/symcfg'] + cmd
        return self.symcmd(cmd, xml=xml)

    def symdisk(self, cmd, xml=True):
        cmd = ['/usr/symcli/bin/symdisk'] + cmd
        return self.symcmd(cmd, xml=xml)

    def symconfigure(self, cmd, xml=True):
        cmd = ['/usr/symcli/bin/symconfigure'] + cmd
        return self.symcmd(cmd, xml=xml)

    def symdev(self, cmd, xml=True):
        cmd = ['/usr/symcli/bin/symdev'] + cmd
        return self.symcmd(cmd, xml=xml)

    def get_sym_info(self):
        out, err, ret = self.symcfg(["list"])
        return out

    def get_sym_dir_info(self):
        out, err, ret = self.symcfg(['-dir', 'all', '-v', 'list'])
        return out

    def get_sym_dev_info(self):
        out, err, ret = self.symdev(['list'])
        return out

    def get_sym_dev_wwn_info(self):
        out, err, ret = self.symdev(['list', '-wwn'])
        return out

    def get_sym_devrdfa_info(self):
        out, err, ret = self.symdev(['list', '-v', '-rdfa'])
        return out

    def get_sym_ficondev_info(self):
        out, err, ret = self.symdev(['list', '-ficon'])
        return out

    def get_sym_meta_info(self):
        out, err, ret = self.symdev(['list', '-meta', '-v'])
        return out

    def get_sym_dev_name_info(self):
        out, err, ret = self.symdev(['list', '-identifier', 'device_name'])
        return out

    def get_sym_disk_info(self):
        out, err, ret = self.symdisk(['list', '-v'])
        return out

    def get_sym_diskgroup_info(self):
        out, err, ret = self.symdisk(['list', '-dskgrp_summary'])
        return out

    def get_sym_pool_info(self):
        out, err, ret = self.symcfg(['-pool', 'list', '-v'])
        return out

    def get_sym_tdev_info(self):
        out, err, ret = self.symcfg(['list', '-tdev', '-detail'])
        return out

    def get_sym_srp_info(self):
        out, err, ret = self.symcfg(['list', '-srp', '-detail', '-v'])
        return out

    def get_sym_slo_info(self):
        out, err, ret = self.symcfg(['list', '-slo', '-detail', '-v'])
        return out

    def parse_xml(self, buff, key=None, as_list=[], exclude=[]):
        tree = fromstring(buff)
        data = []
        def parse_elem(elem, as_list=[], exclude=[]):
            d = {}
            for e in list(elem):
                if e.tag in exclude:
                    continue
                if e.text.startswith("\n"):
                    child = parse_elem(e, as_list, exclude)
                    if e.tag in as_list:
                        if e.tag not in d:
                            d[e.tag] = []
                        d[e.tag].append(child)
                    else:
                        d[e.tag] = child
                else:
                    d[e.tag] = e.text
            return d

        for elem in tree.getiterator(key):
            data.append(parse_elem(elem, as_list, exclude))

        return data

    def list_pools(self, **kwargs):
        print(json.dumps(self.get_pools(), indent=4))

    def get_pools(self, **kwargs):
        out = self.get_sym_pool_info()
        data = self.parse_xml(out, key="DevicePool")
        return data

    def list_tdevs(self, **kwargs):
        print(json.dumps(self.get_tdevs(), indent=4))

    def get_tdevs(self, **kwargs):
        out = self.get_sym_tdev_info()
        data = self.parse_xml(out, key="Device", as_list=["pool"])
        return data

    def list_views(self, **kwargs):
        print(json.dumps(self.get_views(), indent=4))

    def get_views(self, **kwargs):
        out = self.get_sym_view_aclx()
        data = self.parse_xml(out, key="View_Info", as_list=["Initiators", "Director_Identification", "SG", "Device", "dev_port_info"], exclude=["Initiator_List"])
        return data

    def get_initiator_views(self, wwn):
        out = self.get_symaccess(["list", "-type", "initiator", "-wwn", wwn])
        data = self.parse_xml(out, key="view_name")
        return data

    def get_view(self, view):
        out = self.get_symaccess(["show", "view", view])
        data = self.parse_xml(out, key="View_Info")
        return data

    def get_mapping_storage_groups(self, wwn, target):
        l = []
        for view in self.get_initiator_views(wwn):
            view_data = self.get_view(view)
            if "port_info" not in view_data:
                continue
            if "Director_Identification" not in view_data["port_info"]:
                continue
            ports = [e["port_wwn"] for e in view_data["port_info"]["Director_Identification"]]
            if target not in ports:
                continue
            l.append(view_data["stor_grpname"])
        return l

    def translate_mappings(self, mappings):
        sgs = []
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            for target in targets:
                sgs += self.get_mapping_storage_groups(hba_id, target)
        return sgs

class Vmax(Sym):
    def __init__(self, sid, symcli_path, symcli_connect, username, password):
        Sym.__init__(self, sid, symcli_path, symcli_connect, username, password)
        self.keys += ['sym_ig_aclx',
                      'sym_pg_aclx',
                      'sym_sg_aclx',
                      'sym_view_aclx']

        if 'SYMCLI_DB_FILE' in os.environ:
            dir = os.path.dirname(os.environ['SYMCLI_DB_FILE'])
            # flat format
            self.aclx = os.path.join(dir, sid+'.aclx')
            if not os.path.exists(self.aclx):
                # emc grab format
                import glob
                files = glob.glob(os.path.join(dir, sid, sid+'*.aclx'))
                if len(files) == 1:
                    self.aclx = files[0]
            if not os.path.exists(self.aclx):
                print("missing file %s"%self.aclx)
        else:
            self.aclx = None

    def symaccesscmd(self, cmd, xml=True):
        self.set_environ()
        cmd = ['/usr/symcli/bin/symaccess'] + cmd
        if self.aclx is None:
            cmd += ['-sid', self.sid]
        else:
            cmd += ['-file', self.aclx]
        if xml:
            cmd += ['-output', 'xml_element']
        return justcall(cmd)

    def get_sym_pg_aclx(self):
        cmd = ['list', '-type', 'port']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def get_sym_sg_aclx(self):
        cmd = ['list', '-type', 'storage']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def get_sym_ig_aclx(self):
        cmd = ['list', '-type', 'initiator']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def get_sym_view_aclx(self):
        cmd = ['list', 'view', '-detail']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def add_tdev(self, name=None, size=None, **kwargs):
        """
	     create dev count=<n>,
		  size = <n> [MB | GB | CYL],
		  emulation=<EmulationType>,
		  config=<DevConfig>
		  [, preallocate size = <ALL>
		    [, allocate_type = PERSISTENT]]
		  [, remote_config=<DevConfig>, ra_group=<n>]
		  [, sg=<SgName> [, remote_sg=<SgName>]]
		  [, mapping to dir <director_num:port>
		    [starting] target = <scsi_target>,
		    lun=<scsi_lun>, vbus=<fibre_vbus>
		    [starting] base_address = <cuu_address>[,...]]
		  [, device_attr =
		    <SCSI3_PERSIST_RESERV | DIF1 |
		      AS400_GK>[,...]]
		  [, device_name='<DeviceName>'[,number=<n | SYMDEV> ]];
        """

        if size is None:
            raise ex.excError("The '--size' parameter is mandatory")
        size = convert_size(size, _to="MB")
	_cmd = "create dev count=1, size= %d MB, emulation=FBA, config=TDEV, device_attr=SCSI3_PERSIST_RESERV" % (size)
        if name:
            _cmd += ", device_name=%s" % name
	_cmd += ";"
        cmd = ["-cmd", _cmd, "commit", "-noprompt"]
        out, err, ret = self.symconfigure(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("New symdev:"):
                l = line.split()
                if len(l) < 3:
                    raise ex.excError("unable to determine the created SymDevName")
                return {"dev_name": l[2]}
        raise ex.excError("unable to determine the created SymDevName")

    def resize_tdev(self, dev=None, size=None, **kwargs):
        if dev is None:
            raise ex.excError("The '--dev' parameter is mandatory")
        if size is None:
            raise ex.excError("The '--size' parameter is mandatory")
        size = convert_size(size, _to="MB")
        cmd = ["modify", dev, "-tdev", "-cap", str(size), "-captype", "mb", "-noprompt"]
        out, err, ret = self.symdev(cmd, xml=False)
        return out, err, ret

    def del_tdev(self, dev=None, **kwargs):
        if dev is None:
            raise ex.excError("The '--dev' parameter is mandatory")
        cmd = ["delete", dev, "-noprompt"]
        out, err, ret = self.symdev(cmd, xml=False)
        return out, err, ret

    def add_tdev_to_pool(self, dev, pool):
        cmd = ["bind", dev, "-pool", pool, "-noprompt"]
        out, err, ret = self.symdev(cmd)
        return out, err, ret

    def add_tdev_to_sg(self, dev, sg):
        cmd = ["-name", sg, "-type", "storage", "add", dev, "-noprompt"]
        out, err, ret = self.symaccess(cmd)
        return out, err, ret

    def add_disk(self, name=None, size=None, pools=[], mappings={}, **kwargs):
        sgs = self.translate_mappings(mappings)
        if len(sgs) == 0:
            raise ex.excError("no storage group found for the requested mappings")
        data = self.add_tdev(name, size, **kwargs)
        for pool in pools:
            self.add_tdev_to_pool(data["dev_name"], pool)
        for sg in sgs:
            self.add_tdev_to_sg(data["dev_name"], sg)
        return data

class Dmx(Sym):
    def __init__(self, sid):
        Sym.__init__(self, sid)
        self.keys += ['sym_maskdb']

        if 'SYMCLI_DB_FILE' in os.environ:
            dir = os.path.dirname(os.environ['SYMCLI_DB_FILE'])
            # flat format
            self.maskdb = os.path.join(dir, sid+'.bin')
            if not os.path.exists(self.maskdb):
                # emc grab format
                self.maskdb = os.path.join(dir, sid, 'symmaskdb_backup.bin')
            if not os.path.exists(self.maskdb):
                print("missing file %s"%self.maskdb)
        else:
            self.maskdb = None

    def symaccesscmd(self, cmd, xml=True):
        self.set_environ()
        cmd = ['/usr/symcli/bin/symaccess'] + cmd
        if self.maskdb is None:
            cmd += ['-sid', self.sid]
        else:
            cmd += ['-f', self.maskdb]
        if xml:
            cmd += ['-output', 'xml_element']
        return justcall(cmd)

    def get_sym_maskdb(self):
        cmd = ['list', 'database']
        out, err, ret = self.symaccesscmd(cmd)
        return out



def do_action(action, array_name=None, node=None, **kwargs):
    o = Arrays()
    array = o.get_array(array_name)
    if array is None:
        raise ex.excError("array %s not found" % array_name)
    if not hasattr(array, action):
        raise ex.excError("not implemented")
    array.node = node
    ret = getattr(array, action)(**kwargs)
    if ret is not None:
        print(json.dumps(ret, indent=4))

def main(argv, node=None):
    parser = OptParser(prog=PROG, options=OPT, actions=ACTIONS,
                       deprecated_actions=DEPRECATED_ACTIONS,
                       global_options=GLOBAL_OPTS)
    options, action = parser.parse_args(argv)
    kwargs = vars(options)
    do_action(action, node=node, **kwargs)

if __name__ == "__main__":
    try:
        ret = main(sys.argv)
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        ret = 1
    sys.exit(ret)

