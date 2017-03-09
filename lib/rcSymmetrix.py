from __future__ import print_function

import sys
import os
import ConfigParser
import json
from xml.etree.ElementTree import XML, fromstring

import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import justcall, which
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
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --targetgroup and --initiatorgroup. Can be specified multiple times."),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Add actions": {
        "add_disk": {
            "msg": "Add and present a thin device",
            "options": [
                OPT.name,
                OPT.size,
                OPT.mappings,
            ],
        },
    },
    "Delete actions": {
        "del_disk": {
            "msg": "Unpresent and delete a thin device",
            "options": [
                OPT.name,
            ],
        },
    },
    "Modify actions": {
        "resize_disk": {
            "msg": "Resize a thin device",
            "options": [
                OPT.name,
                OPT.size,
            ],
        },
    },
    "List actions": {
    },
}


class Arrays(object):
    syms = []

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
                    self.syms.append(Vmax(name, symcli_path, symcli_connect, username, password))
                elif 'DMX' in model or '3000-M' in model:
                    self.syms.append(Dmx(name, symcli_path, symcli_connect, username, password))
                else:
                    print("unsupported sym model: %s" % model, file=sys.stderr)

        del(conf)

    def __iter__(self):
        for array in self.syms:
            yield(array)


class Sym(object):
    def __init__(self, sid, symcli_path, symcli_connect, username, password):
        self.keys = ['sym_info',
                     'sym_dir_info',
                     'sym_dev_info',
                     'sym_dev_wwn_info',
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

    def symcmd(self, cmd):
        self.set_environ()
        cmd += ['-sid', self.sid, '-output', 'xml_element']
        return justcall(cmd)

    def symcfg(self, cmd):
        cmd = [self.symcli_connect, '/usr/symcli/bin/symcfg'] + cmd
        return self.symcmd(cmd)

    def symdisk(self, cmd):
        cmd = [self.symcli_connect, '/usr/symcli/bin/symdisk'] + cmd
        return self.symcmd(cmd)

    def symdev(self, cmd):
        cmd = [self.symcli_connect, '/usr/symcli/bin/symdev'] + cmd
        return self.symcmd(cmd)

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

    def symaccesscmd(self, cmd):
        self.set_environ()
        cmd = ['/usr/symcli/bin/symaccess'] + cmd
        if self.aclx is None:
            cmd += ['-sid', self.sid, '-output', 'xml_element']
        else:
            cmd += ['-file', self.aclx, '-output', 'xml_element']
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

    def symaccesscmd(self, cmd):
        self.set_environ()
        cmd = ['/usr/symcli/bin/symaccess'] + cmd
        if self.maskdb is None:
            cmd += ['-sid', self.sid, '-output', 'xml_element']
        else:
            cmd += ['-f', self.maskdb, '-output', 'xml_element']
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

