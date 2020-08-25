import datetime
import json
import os
from xml.etree.ElementTree import XML

import core.status
from env import Env
from drivers.array.symmetrix import set_sym_env
from utilities.lazy import lazy

import core.exceptions as ex
import utilities.devices.linux

from .. import Sync
from core.objects.svcdict import KEYS

os.environ['PATH'] += ":/usr/symcli/bin"
set_sym_env()

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "symsrdfs"
KEYWORDS = [
    {
        "keyword": "symid",
        "at": True,
        "required": True,
        "text": "Id of the local symmetrix array hosting the symdg. This parameter is usually scoped to define different array ids for different nodes."
    },
    {
        "keyword": "symdg",
        "at": False,
        "required": True,
        "text": "Name of the symmetrix device group where the source and target devices are grouped."
    },
    {
        "keyword": "rdfg",
        "at": False,
        "convert": "integer",
        "required": True,
        "text": "Name of the RDF group pairing the source and target devices."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("symdg"):
        data.append("sync.symsrdfs")
    return data


class SyncSymsrdfs(Sync):
    def __init__(self,
                 symid=None,
                 symdg=None,
                 rdfg=None,
                 symdevs=None,
                 precopy_timeout=300,
                 **kwargs):
        super(SyncSymsrdfs, self).__init__(type="sync.symsrdfs", **kwargs)

        if symdevs is None:
            symdevs = []
        self.pausable = False
        self.label = "srdf/s symdg %s"%(symdg)
        self.symid = symid

        self.symdg = symdg
        self.rdfg = rdfg
        self.symdevs = symdevs
        self.precopy_timeout = precopy_timeout
        self.symdev = {}
        self.pdevs = {}
        self.svcstatus = {}
        self.symld = {}
        self.pairs = []
        self._pairs = []
        self.active_pairs = []
        self.last = None

    def __str__(self):
        return "%s symdg=%s symdevs=%s rdfg=%s" % (
            super(SyncSymsrdfs, self).__str__(),
            self.symdg,
            self.symdevs,
            self.rdfg
        )

    def list_pd(self):
        """
        <?xml version="1.0" standalone="yes" ?>
        <SymCLI_ML>
          <Inquiry>
            <Dev_Info>
              <pd_name>/dev/sdb</pd_name>
              <dev_name>000F1</dev_name>
              <symid>000196801561</symid>
              <dev_ident_name>V_TOOLSDL360S24</dev_ident_name>
            </Dev_Info>
            <Product>
              <vendor>EMC</vendor>
            </Product>
          </Inquiry>
        """
        inq = {}
        cmd = ["syminq", "-identifier", "device_name", "-output", "xml_e"]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        xml = XML(out)
        for e in xml.findall("Inquiry/Dev_Info"):
            pd_name = e.find("pd_name").text
            dev_name = e.find("dev_name").text
            if dev_name not in inq:
                inq[dev_name] = []
            inq[dev_name].append(pd_name)

        """
        <?xml version="1.0" standalone="yes" ?>
        <SymCLI_ML>
          <DG>
            <Device>
              <Dev_Info>
                <dev_name>003AD</dev_name>
                <configuration>RDF1+TDEV</configuration>
                <ld_name>DEV001</ld_name>
                <status>Ready</status>
              </Dev_Info>
              <Front_End>
                <Port>
                  <pd_name>/dev/sdq</pd_name>
                  <director>07E</director>
                  <port>1</port>
                </Port>
              </Front_End>
            </Device>
          </DG>
        </SymCLI_ML>
        """
        cmd = ['symdg', '-g', self.symdg, 'list', 'ld', '-output', 'xml_e',
               '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        devs = []
        xml = XML(out)
        for e in xml.findall("DG/Device/Dev_Info"):
            dev_name = e.find("dev_name").text
            if dev_name in inq:
                devs += inq[dev_name]

        return devs

    def promote_devs_rw(self):
        if Env.sysname != "Linux":
            return
        devs = self.list_pd()
        devs = [d for d in devs if d.startswith("/dev/mapper/") or d.startswith("/dev/dm-") or d.startswith("/dev/rdsk/")]
        for dev in devs:
            self.promote_dev_rw(dev)

    def promote_dev_rw(self, dev):
        utilities.devices.linux.promote_dev_rw(dev, log=self.log)

    def get_symid_from_export(self, cf):
        with open(cf, 'r') as f:
            buff = f.read()
        return buff.split("\n")[0].split()[-1]

    def postsync(self):
        local_export_symid = self.get_symid_from_export(self.dgfile_local_name)
        if local_export_symid == self.symid:
            return self.do_dgimport(self.dgfile_local_name)
        remote_export_symid = self.get_symid_from_export(self.dgfile_rdf_name)
        if remote_export_symid == self.symid:
            self.do_dgimport(self.dgfile_rdf_name)

    def presync(self):
        s = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))
        if self.svc.options.force or s['avail'].status == core.status.UP:
            self.do_rdf_dgexport()
            self.do_local_dgexport()
            self.do_dg_wwn_map()

    def files_to_sync(self):
        return [
            self.dgfile_rdf_name,
            self.dgfile_local_name,
            self.wwn_map_fpath,
        ]

    @lazy
    def wwn_map_fpath(self):
        return os.path.join(self.var_d, "wwn_map")

    def do_dg_wwn_map(self):
        devs = []
        with open(self.dgfile_local_name, "r") as filep:
            for line in filep.readlines():
                if "DEV" not in line:
                    continue
                devs.append(line.split()[1])
        cmd = ["/usr/symcli/bin/symdev", "list", "-output", "xml_e", "-sid", self.symid, "-devs", ",".join(devs), "-v"]
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        tree = XML(out)
        mapping = []
        for dev in tree.findall("Symmetrix/Device"):
            try:
                local = dev.find('Product/wwn').text
                remote = dev.find('RDF/Remote/wwn').text
            except Exception as exc:
                self.log.warning(str(exc))
            else:
                mapping.append((local, remote))
        with open(self.wwn_map_fpath, 'w') as filep:
            json.dump(mapping, filep)
            filep.write("\n")

    def do_local_dgexport(self, fpath=None):
        if fpath is None:
            fpath = self.dgfile_local_name
        try:
            os.unlink(fpath)
        except:
            pass
        cmd = ['/usr/symcli/bin/symdg', 'export', self.symdg, '-f', fpath,
               '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        return out

    def do_rdf_dgexport(self):
        fpath = self.dgfile_rdf_name
        try:
            os.unlink(fpath)
        except:
            pass
        cmd = ['/usr/symcli/bin/symdg', 'export', self.symdg, '-f', fpath,
               '-rdf', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        return out

    def do_dgremove(self):
        cmd = ['/usr/symcli/bin/symdg', 'delete', self.symdg, '-force',
               '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        return out

    def is_dgimport_needed(self):
        self.do_local_dgexport(fpath=self.dgfile_tmp_local_name)
        import filecmp
        if filecmp.cmp(self.dgfile_tmp_local_name, self.dgfile_rdf_name, shallow=False):
            return False
        return True

    def do_dgimport(self, ef):
        if self.symdg in self.get_dg_list():
            if not self.is_dgimport_needed():
                self.log.info("symrdf dg %s is already up to date"%self.symdg)
                return
            else:
                self.do_dgremove()
        self.log.info("symrdf dg %s will be imported from file"%self.symdg)
        cmd = ['symdg', 'import', self.symdg, '-f', ef, '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        return out

    @lazy
    def dgfile_tmp_local_name(self):
        return os.path.join(self.var_d, 'symrdf_' + self.symdg + '.dg.tmp.local')

    @lazy
    def dgfile_local_name(self):
        return os.path.join(self.var_d, 'symrdf_' + self.symdg + '.dg.local')

    @lazy
    def dgfile_rdf_name(self):
        return os.path.join(self.var_d, 'symrdf_' + self.symdg + '.dg.rdf')

    def flush_cache(self):
        self.unset_lazy("rdf_query")

    def get_symdevs(self):
        for symdev in self.symdevs:
            l = symdev.split(':')
            if len(l) != 2:
                self.log.error("symdevs must be in symid:symdev ... format")
                raise ex.Error
            self.symdev[l[0],l[1]] = dict(symid=l[0], symdev=l[1])

    @lazy
    def rdf_query(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'query', '-output', 'xml_e']
        ret, out, err = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd))
        return out

    def dg_query(self):
        cmd = ['/usr/symcli/bin/symdg', 'list', '-output', 'xml_e',
               '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        return out

    # browse local device groups and build dict with list
    def get_dg_list(self):
        try:
            rdf_query = self.dg_query()
        except:
            return {}
        self.xmldg = XML(rdf_query)
        self.dglist = {}
        for dg in self.xmldg.findall("DG/DG_Info"):
            name = dg.find('name').text
            self.dglist[name] = None
        return self.dglist

    def get_dg_rdf_type(self):
        rdf_query = self.rdf_query
        self.xmldg = XML(rdf_query)
        rdftype = self.xmldg.find('DG/DG_Info/type').text
        return rdftype

    def is_rdf1_dg(self):
        if self.get_dg_rdf_type() == "RDF1":
            return True
        return False

    def is_rdf2_dg(self):
        if self.get_dg_rdf_type() == "RDF2":
            return True
        return False

    def is_rdf21_dg(self):
        if self.get_dg_rdf_type() == "RDF21":
            return True
        return False


    def get_dg_state(self):
        h = {}
        for pair in self.xmldg.findall("DG/RDF_Pair"):
            mode = pair.find('mode').text
            state = pair.find('pair_state').text
            key = mode + "/" + state
            h[key] = None
        if len(h) == 1:
            retmsg = list(h.keys())[0]
        else:
            retmsg = "mixed srdf pairs state"
        return retmsg

    def get_rdfpairs_from_dg(self):
        cmd = ['symrdf', '-g', self.symdg, '-rdfg', str(self.rdfg), 'query',
               '-output', 'xml_e']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.Error

        self.rdfpairs = {}   # remote_symm;remote_dev;rdfg
        self.xmldg = XML(out)

        for pair in self.xmldg.findall("DG/RDF_Pair"):
            source = pair.find('Source/dev_name').text
            target = pair.find('Target/dev_name').text
            self.rdfpairs[source] = target
        self.log.debug("rdfpairs from dg %s", str(self.rdfpairs))

    def is_synchronous_mode(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-synchronous', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_asynchronous_mode(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-asynchronous', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_acp_disk_mode(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-acp_disk', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_synchronized_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-synchronized', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_synchronous_and_synchronized_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-synchronous', '-synchronized',
               '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_syncinprog_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-syncinprog', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_suspend_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-suspended', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_split_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-split', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_failedover_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-failedover', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_partitioned_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-partitioned', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    # SRDF/A expected state is consistent AND enabled
    def is_consistent_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-consistent', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def is_enabled_state(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), 'verify', '-enabled', '-i', '15', '-c', '4']
        (ret, out, err) = self.call(cmd)
        if ret == 0:
            return True
        return False

    def can_sync(self, target=None):
        return True

    def resume(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), '-noprompt', 'resume', '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        self.flush_cache()

    def suspend(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg' ,
               str(self.rdfg), '-noprompt', 'suspend', '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        self.flush_cache()

    def establish(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), '-noprompt', 'establish', '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        self.flush_cache()

    def failover(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), '-noprompt', 'failover', '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        self.flush_cache()

    def failoverestablish(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), '-noprompt', 'failover', '-establish',
               '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error("Failed to run command %s"% ' '.join(cmd) )
        self.flush_cache()

    def split(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), '-noprompt', 'split', '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.flush_cache()

    def swap(self):
        cmd = ['/usr/symcli/bin/symrdf', '-g', self.symdg, '-rdfg',
               str(self.rdfg), '-noprompt', 'swap', '-i', '15', '-c', '4']
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.Error
        self.flush_cache()

    def get_syminfo(self):
        self.get_dg_rdf_type()

    def get_last(self):
        if self.last is not None:
            return
        for symid, symdev in self.symdev:
            ld = self.symld[symid,symdev]
            # format: Thu Feb 25 10:20:56 2010
            last = datetime.datetime.strptime(ld['clone_lastaction'], "%a %b %d %H:%M:%S %Y")
            if self.last is None or last > self.last:
                self.last = last

    def sync_status(self, verbose=False):
        try:
            self.get_syminfo()
        except ex.Error as e:
            self.status_log(str(e))
            return core.status.WARN
        state = self.get_dg_state()
        self.status_log("current state %s"%state, "info")
        if self.is_synchronous_and_synchronized_state():
            return core.status.UP
        self.status_log("expecting Synchronous/Synchronized")
        return core.status.WARN

    # SRDF split
    def sync_split(self):
        self.split()

    # SRDF suspend
    def sync_quiesce(self):
        self.suspend()

    # SRDF swap
    def sync_swap(self):
        self.swap()

    def sync_break(self):
        self.split()

    # SRDF establish
    def sync_resync(self):
        self.establish()

    def sync_establish(self):
        self.establish()

    def start(self):
        if Env.nodename in self.svc.drpnodes:
            if self.is_rdf2_dg():
                if self.is_synchronous_and_synchronized_state():
                    self.split()
                elif self.is_partitioned_state():
                    self.log.warning("symrdf dg %s is RDF2 and partitioned. failover is preferred action."%self.symdg)
                    self.failover()
                elif self.is_failedover_state():
                    self.log.info("symrdf dg %s is already RDF2 and FailedOver."%self.symdg)
                elif self.is_suspend_state():
                    self.log.warning("symrdf dg %s is RDF2 and suspended: R2 data may be outdated"%self.symdg)
                    self.split()
                elif self.is_split_state():
                    self.log.info("symrdf dg %s is RDF2 and already splitted."%self.symdg)
                else:
                    raise ex.Error("symrdf dg %s is RDF2 on drp node and unexpected SRDF state, you have to manually return to a sane SRDF status.")
            elif self.is_rdf1_dg():
                if self.is_synchronous_and_synchronized_state():
                    pass
                else:
                    raise ex.Error("symrdf dg %s is RDF1 on drp node, you have to manually return to a sane SRDF status.")
        elif Env.nodename in self.svc.nodes:
            if self.is_rdf1_dg():
                if self.is_synchronous_and_synchronized_state():
                    self.log.info("symrdf dg %s is RDF1 and synchronous/synchronized."%self.symdg)
                elif self.is_partitioned_state():
                    self.log.warning("symrdf dg %s is RDF1 and partitioned."%self.symdg)
                elif self.is_failedover_state():
                    raise ex.Error("symrdf dg %s is RDF1 and write protected, you have to manually run either sync_split+sync_establish (ie losing R2 data), or syncfailback (ie losing R1 data)"%self.symdg)
                elif self.is_suspend_state():
                    self.log.warning("symrdf dg %s is RDF1 and suspended."%self.symdg)
                elif self.is_split_state():
                    self.log.warning("symrdf dg %s is RDF1 and splitted."%self.symdg)
                else:
                    raise ex.Error("symrdf dg %s is RDF1 on primary node and unexpected SRDF state, you have to manually return to a sane SRDF status.")
            elif self.is_rdf2_dg():         # start on metrocluster passive node
                if self.is_synchronous_and_synchronized_state():
                    self.failoverestablish()
                elif self.is_partitioned_state():
                    self.log.warning("symrdf dg %s is RDF2 and partitioned, failover is preferred action."%self.symdg)
                    self.failover()
                else:
                    raise ex.Error("symrdf dg %s is RDF2 on primary node, you have to manually return to a sane SRDF status.")
        self.promote_devs_rw()

    def refresh_svcstatus(self):
        self.svcstatus = self.svc.group_status(excluded_groups=set(["app", "sync", "task", "disk.scsireserv"]))

    def get_svcstatus(self):
        if len(self.svcstatus) == 0:
            self.refresh_svcstatus()
