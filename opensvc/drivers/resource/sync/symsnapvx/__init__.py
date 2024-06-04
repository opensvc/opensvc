import core.exceptions as ex

from .. import Sync, notify
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "symsnapvx"
KEYWORDS = [
    {
        "keyword": "symid",
        "required": True,
        "text": "Identifier of the symmetrix array hosting the source and target devices pairs pointed by :kw:`pairs`.",
        "at": True,
    },
    {
        "keyword": "devs",
        "convert": "list",
        "required": True,
        "at": True,
        "text": "Whitespace-separated list of devices ``<src>`` devid to drive with this resource. The destination devices are only needed when the snapshot needs presenting.",
        "example": "00B60 00B63",
    },
    {
        "keyword": "secure",
        "default": True,
        "convert": "boolean",
        "text": "Use :opt:`-secure` in symsnapvx commands.",
        "at": True,
    },
    {
        "keyword": "absolute",
        "text": "Use :opt:`-delta` in symsnapvx commands.",
        "at": True,
    },
    {
        "keyword": "delta",
        "text": "Use :opt:`-delta` in symsnapvx commands.",
        "at": True,
    },
    {
        "keyword": "name",
        "text": "Use :opt:`-name` in symsnapvx commands.",
        "at": True,
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
    if which("symsnapvx"):
        data.append("sync.symsnapvx")
    return data

"""
Example: symsnapvx list -output xml_e

<?xml version="1.0" standalone="yes" ?>
<SymCLI_ML>
  <Symmetrix>
    <Symm_Info>
      <symid>000111111111</symid>
      <microcode_version>5978</microcode_version>
    </Symm_Info>
    <Snapvx>
      <Snapshot>
        <source>00097</source>
        <snapshot_name>SNAP_1</snapshot_name>
        <last_timestamp>Fri May 31 06:15:05 2024</last_timestamp>
        <num_generations>20</num_generations>
        <link>No</link>
        <restore>No</restore>
        <failed>No</failed>
        <error_reason>NA</error_reason>
        <GCM>False</GCM>
        <zDP>False</zDP>
        <secured>Yes</secured>
        <expanded>No</expanded>
        <bgdefinprog>No</bgdefinprog>
        <policy>No</policy>
        <persistent>No</persistent>
        <cloud>No</cloud>
      </Snapshot>

"""
def parse_vx_list(s):
    l = []
    etree = ElementTree.fromstring(s)
    for e in etree.findall("Symmetrix/Snapvx/Snapshot"):
        d = {}
        for sub in e.iter():
            d[sub.tag] = sub.text
        d["last"] = parse_last(d.get("last_timestamp"))
        l.append(d)
    return l

def parse_last(s):
    # format: Thu Feb 25 10:20:56 2010
    try:
        return datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %Y")
    except AttributeError:
        return

class SyncSymsnapvx(Sync):
    def __init__(self,
                 type="sync.symsnapvx",
                 symid=None,
                 devs=None,
                 secure=None,
                 absolute=None,
                 delta=None,
                 name=None,
                 **kwargs):
        super(SyncSymsnapvx, self).__init__(type=type, **kwargs)

        self.symid = symid
        self.devs = devs or []
        self.secure = secure or False
        self.absolute = absolute
        self.delta = delta
        self.name = name
        self.default_schedule = "@0"

        if name:
            self.label = "symsnapvx symid %s %s" % (symid, name)
        else:
            self.label = "symsnapvx symid %s devs %s" % (symid, " ".join(devs))
        if len(self.label) > 80:
            self.label = self.label[:76] + "..."

    def __str__(self):
        return "%s symid=%s devs=%s" % (
            super(SyncSymclone, self).__str__(),
            self.symid,
            str(self.devs)
        )

    def _info(self):
        data = [
          ["devs", ",".join(self.devs)],
          ["delta", str(self.delta)],
          ["absolute", str(self.absolute)],
          ["secure", str(self.secure)],
          ["name", str(self.name)],
          ["symid", str(self.symid)],
        ]
        return data

    def vx_cmd(self):
        return ["/usr/symcli/bin/symsnapvx", "-sid", self.symid]

    def list(self):
        if not self.devs:
            raise ex.Error("devs is mandatory")
        cmd = self.vx_cmd() + ["list", ",".join(self.devs)]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("vx_list: %s" % err)
        return parse_vx_list(out)

    def establish(self):
        cmd = self.vx_cmd() + ["establish"]
        if self.secure:
            cmd += ["-secure"]
        if self.delta and self.absolute:
            raise ex.Error("set delta or absolute, not both")
        if self.delta:
            cmd += ["-ttl", "-delta", self.delta]
        if self.absolute:
            cmd += ["-ttl", "-absolute", self.absolute]
        cmd += ["-name", self.format_name()]
        ret, out, err = self.vcall(cmd, warn_to_info=True)
        if ret != 0:
            raise ex.Error(err)

    def format_name(self):
        if self.name:
            return self.name
        else:
            # 1.svc1.ns1.svc.cluster1
            ns = "root" if self.svc.namespace is None else self.svc.namespace
            return "%s.%s.%s.%s.%s" % (self.rid.split("#")[-1], self.svc.name, ns, self.svc.kind, self.svc.node.cluster_name)

    def can_sync(self, target=None):
        try:
            self.check_requires("sync_update")
        except ex.Error as e:
            self.log.debug(e)
            return False
        return True

    def last(self, snaps):
        oldest = None
        for snap in snaps:
            last = snap["last"]
            if last is None:
                continue
            if oldest is None or last > oldest:
                oldest = last
        return oldest

    def snap_errors(self, snaps):
        for snap in snaps:
            err = d.get("error_reason")
            if err != "NA":
                source = d.get("source")
                self.status_log("%s: %s", source, err)

    def _status(self, verbose=False):
        snaps = self.list()
        self.snap_errors(snaps)
        last = self.last(snaps)
        if last is None:
            return core.status.DOWN
        self.status_log("Last sync on %s" % self.last, "info")
        return core.status.UP

    @notify
    def sync_update(self):
        self.establish()

