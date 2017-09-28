from __future__ import print_function

import logging
import sys
import os
import ConfigParser
import json
import time
from xml.etree.ElementTree import XML, fromstring

import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import justcall, which
from converters import convert_size
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
        "--name", action="store", dest="name",
        help="The device identifier name (ex: mysvc_1)"),
    "dev": Option(
        "--dev", action="store", dest="dev",
        help="The device id (ex: 00A04)"),
    "force": Option(
        "--force", action="store_true", dest="force",
        help="bypass the downsize sanity check."),
    "pair": Option(
        "--pair", action="store", dest="pair",
        help="The device id pair (ex: 00A04:00A04)"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --targetgroup and --initiatorgroup. Can be specified multiple times."),
    "size": Option(
        "--size", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "slo": Option(
        "--slo", action="store", dest="slo",
        help="The thin device Service Level Objective."),
    "srp": Option(
        "--srp", action="store", dest="srp",
        help="The Storage Resource Pool hosting the device."),
    "srdf": Option(
        "--srdf", action="store_true", dest="srdf",
        help="Create a SRDF mirrored device pair. The array pointed by --array is will host the R1 member."),
    "rdfg": Option(
        "--rdfg", action="store", dest="rdfg",
        help="The RDF / RA Group number, required if --srdf is set."),
    "sg": Option(
        "--sg", action="store", dest="sg",
        help="As an alternative to --mappings, specify the storage group to put the dev into."),
    "invalidate": Option(
        "--invalidate", action="store", dest="invalidate",
        help="The SRDF mirror member to invalidate upon createpair (ex: R2). Don't set to just establish."),
    "srdf_type": Option(
        "--srdf-type", action="store", dest="srdf_type",
        help="The device role in the SRDF mirror (ex: R1)"),
    "srdf_mode": Option(
        "--srdf-mode", action="store", dest="srdf_mode",
        help="Device mirroring mode. Either sync, acp_wp or acp_disk"),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Generic actions": {
        "add_disk": {
            "msg": "Add and present a thin device.",
            "options": [
                OPT.name,
                OPT.size,
                OPT.mappings,
                OPT.slo,
                OPT.srp,
                OPT.srdf,
                OPT.rdfg,
            ],
        },
        "add_map": {
            "msg": "Present a device.",
            "options": [
                OPT.dev,
                OPT.mappings,
                OPT.slo,
                OPT.srp,
                OPT.sg,
            ],
        },
        "del_disk": {
            "msg": "Unpresent and delete a thin device.",
            "options": [
                OPT.dev,
            ],
        },
        "del_map": {
            "msg": "Unpresent a device.",
            "options": [
                OPT.dev,
            ],
        },
        "rename_disk": {
            "msg": "Rename a device.",
            "options": [
                OPT.dev,
                OPT.name,
            ],
        },
        "resize_disk": {
            "msg": "Resize a thin device.",
            "options": [
                OPT.dev,
                OPT.force,
                OPT.size,
            ],
        },
    },
    "Low-level actions": {
        "add_tdev": {
            "msg": "Add a thin device. No masking.",
            "options": [
                OPT.name,
                OPT.size,
            ],
        },
        "createpair": {
            "msg": "Delete the SRDF pairing for device.",
            "options": [
                OPT.pair,
                OPT.rdfg,
                OPT.invalidate,
                OPT.srdf_mode,
                OPT.srdf_type,
            ],
        },
        "del_tdev": {
            "msg": "Delete a thin device. No unmasking.",
            "options": [
                OPT.dev,
            ],
        },
        "deletepair": {
            "msg": "Delete the SRDF pairing for device.",
            "options": [
                OPT.dev,
            ],
        },
        "list_pools": {
            "msg": "List thin pools.",
        },
        "list_sgs": {
            "msg": "List storage groups.",
        },
        "list_tdevs": {
            "msg": "List thin devices.",
            "options": [
                OPT.dev,
            ],
        },
        "list_views": {
            "msg": "List views, eg. groups of initiators/targets/devices.",
            "options": [
                OPT.dev,
            ],
        },
        "set_mode": {
            "msg": "Set the device pair rdf mode.",
            "options": [
                OPT.dev,
                OPT.srdf_mode,
            ],
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
        cf = rcEnv.paths.authconf
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)

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
                symcli_path = self.find_symcli_path()

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
        self.keys = [
            'sym_info',
            'sym_dir_info',
            'sym_rdfg_info',
            'sym_dev_info',
            'sym_dev_wwn_info',
            'sym_dev_name_info',
            'sym_devrdfa_info',
            'sym_ficondev_info',
            'sym_meta_info',
            'sym_disk_info',
            'sym_diskgroup_info',
        ]
        self.sid = sid
        self.symcli_path = symcli_path
        self.symcli_connect = symcli_connect
        self.username = username
        self.password = password
        self.log = logging.getLogger(rcEnv.nodename+".array.sym."+self.sid)

    def set_environ(self):
        if self.symcli_connect:
            os.environ["SYMCLI_CONNECT"] = self.symcli_connect
        elif "SYMCLI_CONNECT" in os.environ:
            del os.environ["SYMCLI_CONNECT"]

    def symcmd(self, cmd, xml=True, log=False):
        self.set_environ()
        cmd += ['-sid', self.sid]
        if xml:
            cmd += ['-output', 'xml_element']
        if log and self.node:
            self.log.info(" ".join(cmd))
        return justcall(cmd)

    def symsg(self, cmd, xml=True, log=False):
        cmd = ['/usr/symcli/bin/symsg'] + cmd
        return self.symcmd(cmd, xml=xml, log=log)

    def symcfg(self, cmd, xml=True, log=False):
        cmd = ['/usr/symcli/bin/symcfg'] + cmd
        return self.symcmd(cmd, xml=xml, log=log)

    def symdisk(self, cmd, xml=True):
        cmd = ['/usr/symcli/bin/symdisk'] + cmd
        return self.symcmd(cmd, xml=xml)

    def symconfigure(self, cmd, xml=True, log=False):
        cmd = ['/usr/symcli/bin/symconfigure'] + cmd
        return self.symcmd(cmd, xml=xml, log=log)

    def symdev(self, cmd, xml=True, log=False):
        cmd = ['/usr/symcli/bin/symdev'] + cmd
        return self.symcmd(cmd, xml=xml, log=log)

    def symrdf(self, cmd, xml=True, log=False):
        cmd = ['/usr/symcli/bin/symrdf'] + cmd
        return self.symcmd(cmd, xml=xml, log=log)

    def get_sym_info(self):
        out, err, ret = self.symcfg(["list"])
        return out

    def get_sym_rdfg_info(self):
        out, err, ret = self.symcfg(['-rdfg', 'all', 'list'])
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

    def get_sym_sg_info(self):
        out, err, ret = self.symsg(['list', '-v'])
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
                    if e.tag in as_list:
                        if e.tag not in d:
                            d[e.tag] = []
                        d[e.tag].append(e.text)
                    else:
                        d[e.tag] = e.text
            return d

        for elem in tree.getiterator(key):
            data.append(parse_elem(elem, as_list, exclude))

        return data

    def get_sym_dev_wwn(self, dev):
        out, err, ret = self.symdev(['list', '-devs', dev, '-wwn'])
        return self.parse_xml(out, key="Device")

    def get_sym_dev_show(self, dev):
        out, err, ret = self.symdev(['show', dev])
        return self.parse_xml(out, key="Device")

    def list_pools(self, **kwargs):
        print(json.dumps(self.get_pools(), indent=4))

    def get_pools(self, **kwargs):
        out = self.get_sym_pool_info()
        data = self.parse_xml(out, key="DevicePool")
        return data

    def get_sgs(self, **kwargs):
        out = self.get_sym_sg_info()
        data = self.parse_xml(out, key="SG_Info")
        return data

    def list_sgs(self, **kwargs):
        print(json.dumps(self.get_sgs(), indent=4))

    def load_names(self):
        out = self.get_sym_dev_name_info()
        l = self.parse_xml(out, key="Dev_Info")
        self.dev_names = {}
        for d in l:
            self.dev_names[d["dev_name"]] = d["dev_ident_name"]

    def list_tdevs(self, dev=None, **kwargs):
        self.load_names()
        data = self.get_tdevs(dev)
        for i, d in enumerate(data):
            if d["dev_name"] not in self.dev_names:
                continue
            data[i]["dev_ident_name"] = self.dev_names[d["dev_name"]]
        print(json.dumps(data, indent=4))

    def get_tdevs(self, dev=None, **kwargs):
        if dev:
            out, err, ret = self.symcfg(['list', '-devs', dev, '-tdev', '-detail'])
        else:
            out, err, ret = self.symcfg(['list', '-tdev', '-detail'])
        data = self.parse_xml(out, key="Device", as_list=["pool"])
        return data

    def list_views(self, dev=None, **kwargs):
        if dev is None:
            print(json.dumps(self.get_views(), indent=4))
            return
        views = self.get_dev_views(dev)
        l = []
        for view in views:
            out, err, ret = self.symaccesscmd(["show", "view", view])
            if out.strip() == "":
                continue
            l.append(self.parse_xml(out, key="View_Info", as_list=["Initiators", "Director_Identification", "SG", "Device", "dev_port_info"], exclude=["Initiator_List"]))
        print(json.dumps(l, indent=4))

    def get_views(self, **kwargs):
        out = self.get_sym_view_aclx()
        if out.strip() == "":
            return []
        data = self.parse_xml(out, key="View_Info", as_list=["Initiators", "Director_Identification", "SG", "Device", "dev_port_info"], exclude=["Initiator_List"])
        return data

    def get_dev_views(self, dev):
        sgs = self.get_dev_sgs(dev)
        views = set()
        for sg in sgs:
            out, err, ret = self.symaccesscmd(["show", sg, "-type", "storage"])
            if out.strip() == "":
                continue
            data = self.parse_xml(out, key="Mask_View_Names", as_list=["view_name"])
            for d in data:
                if "view_name" not in d:
                    continue
                views |= set(d["view_name"])
        return views

    def get_initiator_views(self, wwn):
        out, err, ret = self.symaccesscmd(["list", "-type", "initiator", "-wwn", wwn])
        if out.strip() == "":
            return []
        data = self.parse_xml(out, key="Mask_View_Names", as_list=["view_name"])
        views = set()
        for d in data:
            if "view_name" not in d:
                continue
            for view_name in d["view_name"]:
                views.add(view_name.rstrip(" *"))
        return views

    def get_view(self, view):
        out, err, ret = self.symaccesscmd(["show", "view", view])
        data = self.parse_xml(out, key="View_Info", as_list=["Director_Identification", "SG", "Initiator", "Device", "dev_port_info"], exclude=["Initiators"])
        if len(data) == 0:
            return
        return data[0]

    def get_mapping_storage_groups(self, hba_id, tgt_id):
        l = set()
        for view in self.get_initiator_views(hba_id):
            view_data = self.get_view(view)
            if view_data is None:
                continue
            if "port_info" not in view_data:
                continue
            if "Director_Identification" not in view_data["port_info"]:
                continue
            ports = [e["port_wwn"] for e in view_data["port_info"]["Director_Identification"]]
            if tgt_id not in ports:
                continue
            view_sgs = []
            if "SG_Child_info" in view_data and "SG" in view_data["SG_Child_info"] and len(view_data["SG_Child_info"]["SG"]) > 0:
                for sg_data in view_data["SG_Child_info"]["SG"]:
                    view_sgs.append(sg_data["group_name"])
            else:
                view_sgs.append(view_data["stor_grpname"])
            for sg in view_sgs:
                if sg not in self.sg_mappings:
                    self.sg_mappings[sg] = []
                if sg not in self.sg_initiator_count:
                    self.sg_initiator_count[sg] = 0
                self.sg_initiator_count[sg] += len(view_data["Initiator_List"]["Initiator"])
                self.sg_mappings[sg].append({
                    "sg": sg,
                    "view_name": view_data["view_name"],
                    "hba_id": hba_id,
                    "tgt_id": tgt_id,
                })
                l.add(sg)
        return l

    def narrowest_sg(self, sgs):
        if len(sgs) == 0:
            return
        if len(sgs) == 1:
            return sgs[0]
        narrowest = sgs[0]
        for sg in sgs[1:]:
            if self.sg_initiator_count[sg] < self.sg_initiator_count[narrowest]:
                narrowest = sg
        return narrowest

    def translate_mappings(self, mappings):
        sgs = None
        if mappings is None:
            return sgs
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            for tgt_id in targets:
                _sgs = self.get_mapping_storage_groups(hba_id, tgt_id)
                if sgs is None:
                    sgs = _sgs
                else:
                    sgs &= _sgs
        return sgs

class Vmax(Sym):
    def __init__(self, sid, symcli_path, symcli_connect, username, password):
        Sym.__init__(self, sid, symcli_path, symcli_connect, username, password)
        self.keys += [
            'sym_ig_aclx',
            'sym_pg_aclx',
            'sym_sg_aclx',
            'sym_view_aclx',
            'sym_pool_info',
            'sym_tdev_info',
            'sym_sg_info',
            'sym_srp_info',
            'sym_slo_info',
        ]
        self.sg_mappings = {}
        self.sg_initiator_count = {}

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
                print("missing file %s"%self.aclx, file=sys.stderr)
        else:
            self.aclx = None

    def symaccesscmd(self, cmd, xml=True, log=False):
        self.set_environ()
        cmd = ['/usr/symcli/bin/symaccess'] + cmd
        if self.aclx is None:
            cmd += ['-sid', self.sid]
        else:
            cmd += ['-file', self.aclx]
        if xml:
            cmd += ['-output', 'xml_element']
        if log and self.node:
            self.log.info(" ".join(cmd))
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

    def write_temp_file(self, content):
        import tempfile
        try:
            tmpf = tempfile.NamedTemporaryFile()
            fpath = tmpf.name
            tmpf.close()
            with open(fpath, "w") as tmpf:
                tmpf.write(content)
        except (OSError, IOError) as exc:
            raise ex.excError("failed to write temp file: %s" % str(exc))
        return fpath

    def add_tdev(self, name=None, size=None, srdf=False, rdfg=None, **kwargs):
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
        _cmd = "create dev count=1, size= %d MB, emulation=FBA, device_attr=SCSI3_PERSIST_RESERV" % size

        if srdf and rdfg:
            _cmd += ", config=RDF1+TDEV, remote_config=RDF2+TDEV, ra_group=%s" % str(rdfg)
        elif srdf and rdfg is None:
            raise ex.excError("--srdf is specified but --rdfg is not")
        else:
            _cmd += ", config=TDEV"

        if name:
            _cmd += ", device_name=%s" % name
        _cmd += ";"
        cmd = ["-cmd", _cmd, "commit", "-noprompt"]
        out, err, ret = self.symconfigure(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.excError(err)
        """
            out contains:
            ...
            New symdev:  003AF [TDEV]
            ...
        """
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("New symdev:"):
                l = line.split()
                if len(l) < 3:
                    raise ex.excError("unable to determine the created SymDevName")
                dev = l[2]
                if srdf:
                    self.set_mode(dev)
                data = self.get_sym_dev_wwn(dev)[0]
                return data
        raise ex.excError("unable to determine the created SymDevName")

    def get_dev_rdf(self, dev):
        data = self.get_sym_dev_show(dev)
        if len(data) != 1:
            raise ex.excError("device %s does not exist" % dev)
        data = data[0]
        if "RDF" not in data or "Local" not in data["RDF"] or "Remote" not in data["RDF"]:
            raise ex.excError("device %s is not handled by srdf" % dev)
        return data["RDF"]

    def write_dev_pairfile(self, dev, rdev):
        content = dev + " " + rdev + "\n"
        self.log.info("write pair file with content: %s", content.strip())
        fpath = self.write_temp_file(content)
        return fpath

    def set_mode(self, dev, **kwargs):
        data = self.get_dev_rdf(dev)
        rdfg = data["Local"]["ra_group_num"]
        rdev = data["Remote"]["dev_name"]
        fpath = self.write_dev_pairfile(dev, rdev)
        cmd = ["-f", fpath, "-rdfg", rdfg, "set", "mode", "sync", "-noprompt"]
        out, err, ret = self.symrdf(cmd, xml=False, log=True)
        os.unlink(fpath)
        if ret != 0:
            raise ex.excError(out+err)

    def createpair(self, pair=None, rdfg=None, srdf_mode=None, srdf_type=None, invalidate=None, **kwargs):
        if pair is None:
            raise ex.excError("the --pair argument is mandatory")
        if srdf_type is None:
            raise ex.excError("the --srdf-type argument is mandatory")
        if srdf_mode is None:
            raise ex.excError("the --srdf-mode argument is mandatory")
        if pair.count(":") != 1:
            raise ex.excError("misformatted pair %s" % pair)
        dev, rdev = pair.split(":")
        try:
            rdf_data = self.get_dev_rdf(dev)
        except ex.excError:
            rdf_data = None
        if rdf_data is not None:
            raise ex.excError("dev %s is already is in a RDF relation" % dev)
        fpath = self.write_dev_pairfile(dev, rdev)
        cmd = ["-f", fpath, "-rdfg", rdfg, "createpair", "-noprompt", "-rdf_mode", srdf_mode, "-type", srdf_type]
        if invalidate in ("R1", "R2"):
            cmd += ["-invalidate", invalidate]
        else:
            cmd += ["-establish"]
        out, err, ret = self.symrdf(cmd, xml=False, log=True)
        if ret != 0:
            os.unlink(fpath)
            raise ex.excError(out+err)
        os.unlink(fpath)

    def deletepair(self, dev=None, **kwargs):
        try:
            data = self.get_dev_rdf(dev)
        except ex.excError:
            return
        rdfg = data["Local"]["ra_group_num"]
        rdev = data["Remote"]["dev_name"]
        fpath = self.write_dev_pairfile(dev, rdev)
        if data["RDF_Info"]["pair_state"] != "Suspended":
            cmd = ["-f", fpath, "-rdfg", rdfg, "suspend", "-noprompt"]
            out, err, ret = self.symrdf(cmd, xml=False, log=True)
            if ret != 0:
                os.unlink(fpath)
                raise ex.excError(out+err)
        cmd = ["-f", fpath, "-rdfg", rdfg, "deletepair", "-noprompt", "-force"]
        out, err, ret = self.symrdf(cmd, xml=False, log=True)
        if ret != 0:
            os.unlink(fpath)
            raise ex.excError(out+err)
        os.unlink(fpath)
        return data

    def rename_disk(self, dev=None, name=None, **kwargs):
        if dev is None:
            raise ex.excError("--dev is mandatory")
        if name is None:
            raise ex.excError("--name is mandatory")
        _cmd = "set dev %s device_name='%s';" % (dev, name)
        cmd = ["-cmd", _cmd, "commit", "-noprompt"]
        out, err, ret = self.symconfigure(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.excError(err)

    def resize_disk(self, dev=None, size=None, force=False, **kwargs):
        if dev is None:
            raise ex.excError("The '--dev' parameter is mandatory")
        if size is None:
            raise ex.excError("The '--size' parameter is mandatory")
        dev_data = self.get_sym_dev_show(dev)
        if len(dev_data) != 1:
            raise ex.excError("device %s does not exist" % dev)
        dev_data = dev_data[0]
        current_size = int(dev_data["Capacity"]["megabytes"])
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="MB")
            size = str(current_size + incr)
        else:
            size = str(convert_size(size, _to="MB"))
        if not force and int(size) < current_size:
            raise ex.excError("the target size is smaller than the current "
                              "size. refuse to process. use --force if you "
                              "accept the data loss risk.")
        if "RDF" in dev_data:
            rdf_data = dev_data["RDF"]
        else:
            rdf_data = None
        if rdf_data:
            self.deletepair(dev)
        cmd = ["modify", dev, "-tdev", "-cap", str(size), "-captype", "mb", "-noprompt"]
        out, err, ret = self.symdev(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.excError(err)
        results = {
            "driver_data": {
            },
        }
        if rdf_data:
            results["driver_data"]["rdf"] = rdf_data
        return results

    def del_tdev(self, dev=None, **kwargs):
        if dev is None:
            raise ex.excError("The '--dev' parameter is mandatory")
        data = self.get_sym_dev_wwn(dev)
        if len(data) == 0:
            self.log.info("%s does not exist", dev)
            return
        data = data[0]
        cmd = ["delete", dev, "-noprompt"]
        out, err, ret = self.symdev(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.excError(err)
        self.del_diskinfo(data["wwn"])

    def del_disk(self, dev=None, **kwargs):
        try:
            rdf_data = self.get_dev_rdf(dev)
        except ex.excError as exc:
            self.log.info("rdf data: %s", exc)
            rdf_data = None
        self.set_dev_ro(dev)
        self.del_map(dev)
        self.deletepair(dev)
        retry = 5
        while retry > 0:
            self.free_tdev(dev)
            try:
                self.del_tdev(dev=dev, **kwargs)
                break
            except ex.excError as exc:
                if "A free of all allocations is required" in str(exc):
                    if retry == 1:
                        raise ex.excError("dev %s is still not free of all allocations after 5 tries")
                    # retry
                    retry -= 1
                    time.sleep(5)
                    continue
                raise
        results = {
            "driver_data": {
            },
        }
        if rdf_data:
            results["driver_data"]["rdf"] = rdf_data
        return results

    def del_map(self, dev, **kwargs):
        for sg in self.get_dev_sgs(dev):
            self.del_tdev_from_sg(dev, sg)

    def free_tdev(self, dev):
        while True:
            out, err, ret = self.symdev(["free", "-devs", dev, "-all", "-noprompt"], xml=False, log=True)
            if self.tdev_freed(dev) and not self.tdev_deallocating(dev) and not self.tdev_freeingall(dev):
                break
            time.sleep(5)

    def set_dev_ro(self, dev):
        out, err, ret = self.symdev(["write_disable", dev, "-noprompt"], xml=False, log=True)
        return out, err, ret

    def tdev_freed(self, dev):
        out, err, ret = self.symcfg(["list", "-tdevs", "-devs", dev], xml=True)
        data = self.parse_xml(out, key="Device")
        if len(data) == 0:
            return True
        data = data[0]
        self.log.info("device %s has %s tracks allocated", dev, str(data["alloc_tracks"]))
        if data["alloc_tracks"] in ("0", 0):
            return True
        return False

    def tdev_freeingall(self, dev):
        return self.tdev_status(dev, "-freeingall")

    def tdev_deallocating(self, dev):
        return self.tdev_status(dev, "-deallocating")

    def tdev_status(self, dev, status):
        out, err, ret = self.symcfg(["verify", "-tdevs", "-devs", dev, status], xml=False)
        outv = out.strip().split()
        if len(outv) == 0:
            raise ex.excError("unexpected verify output: %s" % out+err)
        if outv[0] == "None":
            self.log.info("device %s is not %s", dev, status)
            return False
        self.log.info("device %s is %s", dev, status)
        return True

    def add_tdev_to_sg(self, dev, sg):
        if sg is None:
            return
        cmd = ["-name", sg, "-type", "storage", "add", "dev", dev]
        out, err, ret = self.symaccesscmd(cmd, xml=False, log=True)
        if ret != 0:
            self.log.error(err)
        return out, err, ret

    def del_tdev_from_sg(self, dev, sg):
        cmd = ["-name", sg, "-type", "storage", "remove", "dev", dev, "-unmap"]
        out, err, ret = self.symaccesscmd(cmd, xml=False, log=True)
        if ret != 0:
            self.log.error(err)
        return out, err, ret

    def get_dev_sgs(self, dev):
        out, err, ret = self.symaccesscmd(["list", "-type", "storage", "-devs", dev])
        data = self.parse_xml(out, key="Group_Info")
        return [d["group_name"] for d in data if d["Status"] != "IsParent"]

    def get_sg(self, sg):
        out, err, ret = self.symsg(["show", sg])
        data = self.parse_xml(out, key="SG_Info")
        return data[0]

    def filter_sgs(self, sgs, srp=None, slo=None):
        filtered_sgs = []
        if srp is None and slo is None:
            return sgs
        for sg in sgs:
            data = self.get_sg(sg)
            if srp and data["SRP_name"] != srp:
                self.log.info("discard sg %s (srp %s, required %s)", sg, data["SRP_name"], srp)
                continue
            if slo and data["SLO_name"] != slo:
                self.log.info("discard sg %s (slo %s, required %s)", sg, data["SLO_name"], slo)
                continue
            filtered_sgs.append(sg)
        return filtered_sgs

    def get_lun(self, dev, hba_id, tgt_id, view_name):
        view = self.get_view(view_name)
        if view is None:
            return
        port = None
        for port in view["port_info"]["Director_Identification"]:
            if port["port_wwn"] == tgt_id:
                port_id = port["port"]
                break
        if port is None:
            return
        if "Device" not in view:
            return
        for device in view["Device"]:
            if device["dev_name"] != dev:
                continue
            for port in device["dev_port_info"]:
                if port_id == port["port"]:
                    return port["host_lun"]

    def get_mappings(self, dev):
        mappings = {}
        for sg in self.get_dev_sgs(dev):
            for sg, l in self.sg_mappings.items():
                for d in l:
                    d["lun"] = self.get_lun(dev, d["hba_id"], d["tgt_id"], d["view_name"])
                    if d["lun"] is None:
                        continue
                    mappings[d["hba_id"] + ":" + d["tgt_id"]] = d
        return mappings

    def add_disk(self, name=None, size=None, slo=None, srp=None, srdf=False, rdfg=None, mappings=None, **kwargs):
        sg = self.mappings_to_sg(mappings, slo, srp)
        dev_data = self.add_tdev(name, size, srdf, rdfg, **kwargs)
        self._add_map(dev_data["dev_name"], mappings, slo, srp, sg)
        self.push_diskinfo(dev_data, name, size, srp, sg)
        mappings = {}
        results = {
            "disk_id": dev_data["wwn"],
            "disk_devid": dev_data["dev_name"],
            "mappings": self.get_mappings(dev_data["dev_name"]),
            "driver_data": {
                "dev": dev_data,
            },
        }
        return results

    def _add_map(self, dev=None, mappings=None, slo=None, srp=None, sg=None, **kwargs):
        if dev is None:
            raise ex.excError("--dev is mandatory")
        if sg is None:
            sg = self.mappings_to_sg(mappings, slo, srp)
        self.add_tdev_to_sg(dev, sg)

    def add_map(self, dev=None, mappings=None, slo=None, srp=None, sg=None, **kwargs):
        self._add_map(dev, mappings, slo, srp, sg)
        dev_data = self.get_sym_dev_wwn(dev)[0]
        results = {
            "disk_id": dev_data["wwn"],
            "disk_devid": dev_data["dev_name"],
            "mappings": self.get_mappings(dev_data["dev_name"]),
            "driver_data": {
                "dev": dev_data,
            },
        }
        return results

    def mappings_to_sg(self, mappings, slo, srp):
        if mappings is None:
            return
        sgs = self.translate_mappings(mappings)
        if len(sgs) == 0:
            raise ex.excError("no storage group found for the requested mappings")
        sgs = self.filter_sgs(sgs, srp=srp, slo=slo)
        if len(sgs) == 0:
            raise ex.excError("no storage group found for the requested mappings")
        narrowest = self.narrowest_sg(sgs)
        self.log.info("candidates sgs: %s, retain: %s", str(sgs), narrowest)
        return narrowest

    def del_diskinfo(self, disk_id):
        if disk_id in (None, ""):
            return
        if self.node is None:
            return
        try:
            ret = self.node.collector_rest_delete("/disks/%s" % disk_id)
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in ret:
            self.log.error("failed to delete the disk object in the collector: %s", ret["error"])
        return ret

    def push_diskinfo(self, data, name, size, srp, sg):
        if self.node is None:
            return
        try:
            ret = self.node.collector_rest_post("/disks", {
                "disk_id": data["wwn"],
                "disk_devid": data["dev_name"],
                "disk_name": name if name else "",
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.sid,
                "disk_group": srp,
            })
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(ret["error"])
        return ret


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
                print("missing file %s"%self.maskdb, file=sys.stderr)
        else:
            self.maskdb = None

    def symaccesscmd(self, cmd, xml=True, log=False):
        self.set_environ()
        cmd = ['/usr/symcli/bin/symaccess'] + cmd
        if self.maskdb is None:
            cmd += ['-sid', self.sid]
        else:
            cmd += ['-f', self.maskdb]
        if xml:
            cmd += ['-output', 'xml_element']
        if log and self.node:
            self.log.info(" ".join(cmd))
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
    node.log.handlers[1].setLevel(logging.CRITICAL)
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

