#!/opt/opensvc/bin/python

import sys
from rcGlobalEnv import rcEnv
from rcNode import node_get_hostmode

class MissKeyNoDefault(Exception):
     pass

class KeyInvalidValue(Exception):
     pass

class Keyword(object):
    def __init__(self, section, keyword,
                 rtype=None,
                 order=100,
                 required=False,
                 at=False,
                 default=None,
                 validator=None,
                 candidates=None,
                 depends=[],
                 text="",
                 provisioning=False):
        self.section = section
        self.keyword = keyword
        self.rtype = rtype
        self.order = order
        self.at = at
        self.required = required
        self.default = default
        self.candidates = candidates
        self.depends = depends
        self.text = text
        self.provisioning = provisioning

    def __cmp__(self, o):
        if o.order > self.order:
            return -1
        elif o.order == self.order:
            return 0
        return 1

    def __str__(self):
        from textwrap import TextWrapper
        wrapper = TextWrapper(subsequent_indent="%15s"%"", width=78)

        depends = ""
        for d in self.depends:
            depends += "%s in %s\n"%(d[0], d[1])
        if depends == "":
            depends = None

        s = ''
        s += "------------------------------------------------------------------------------\n"
        s += "section:       %s\n"%self.section
        s += "keyword:       %s\n"%self.keyword
        s += "------------------------------------------------------------------------------\n"
        s += "  required:    %s\n"%str(self.required)
        s += "  default:     %s\n"%str(self.default)
        s += "  candidates:  %s\n"%str(self.candidates)
        s += "  depends:     %s\n"%depends
        s += "  scopable:    %s\n"%str(self.at)
        if self.text:
            s += wrapper.fill("  help:        "+self.text)
        if self.at:
            s += "\n\nPrefix the value with '@<node> ', '@nodes ', '@drpnodes ' or '@encapnodes '\n"
            s += "to specify a scope-specific value.\n"
            s += "You will be prompted for new values until you submit an empty value.\n"
        return s

    def form(self, d):
        # skip this form if dependencies are not met
        for d_keyword, d_value in self.depends:
            if d_keyword not in d:
                return d
            if d[d_keyword] not in d_value:
                return d

        # print() the form
        print(self)

        # if we got a json seed, use its values as default
        # else use the Keyword object default
        if self.keyword in d:
            default = d[self.keyword]
        elif self.default is not None:
            default = self.default
        else:
            default = None

        if default is not None:
            default_prompt = " [%s] "%str(default)
        else:
            default_prompt = ""

        req_satisfied = False
        while True:
            try:
                val = raw_input(self.keyword+default_prompt+"> ")
            except EOFError:
                break
            if len(val) == 0:
                if req_satisfied:
                    return d
                if default is None:
                    if self.required:
                        print("value required")
                        continue
                    # keyword is optional, leave dictionary untouched
                    return d
                else:
                    val = default
                if self.candidates is not None and \
                   val not in self.candidates:
                    print("invalid value")
                    continue
                d[self.keyword] = val
            elif self.at and val[0] == '@':
                l = val.split()
                if len(l) < 2:
                    print("invalid value")
                    continue
                val = ' '.join(l[1:])
                d[self.keyword+l[0]] = val
                req_satisfied = True
            else:
                d[self.keyword] = val
                req_satisfied = True
            if self.at:
                # loop for more key@node = values
                print("More '%s' ? <enter> to step to the next parameter."%self.keyword)
                continue
            else:
                return d

class KeywordInteger(Keyword):
    def validator(self, val, d=None):
        try:
             val = int(val)
        except:
             return False
        return True


class KeywordProvision(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="provision",
                  keyword="provision",
                  default="no",
                  candidates=('yes', 'no'),
                  text="Say yes to provision this resource. Warning, provisioning implies destructive operations like formating."
                )

class KeywordMode(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="mode",
                  required=True,
                  order=10,
                  default="hosted",
                  candidates=["hosted", "sg", "vcs", "rhcs"],
                  text="The mode decides upon disposition OpenSVC takes to bring a service up or down : virtualized services need special actions to prepare and boot the container for example, which is not needed for 'hosted' services."
                )

class KeywordPkgName(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="pkg_name",
                  required=False,
                  order=11,
                  depends=[('mode', ["vcs", "sg", "rhcs"])],
                  text="The wrapped cluster package name, as known to the cluster manager in charge."
                )

class KeywordDockerDataDir(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="docker_data_dir",
                  required=False,
                  order=12,
                  text="The directory where the private docker daemon must store its data."
                )

class KeywordContainerType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="type",
                  candidates=rcEnv.vt_supported,
                  text="The type of container.",
                  required=True,
                  order=1
                )

class KeywordDockerRunImage(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="run_image",
                  order=9,
                  required=False,
                  depends=[('type', ["docker"])],
                  text="The docker image pull, and run the container with."
                )

class KeywordDockerRunCommand(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="run_command",
                  order=1,
                  required=False,
                  depends=[('type', ["docker"])],
                  text="The command to execute in the docker container on run."
                )

class KeywordDockerRunArgs(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="run_args",
                  order=2,
                  required=False,
                  depends=[('type', ["docker"])],
                  text="Extra arguments to pass to the docker run command, like volume and port mappings."
                )

class KeywordVirtinst(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="virtinst",
                  depends=[('type', ["kvm", "xen", "ovm"])],
                  text="The virt-install command to use to create the container.",
                  required=True,
                  provisioning=True
                )

class KeywordSnap(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="snap",
                  depends=[('type', ["kvm", "xen", "ovm", "zone", "esx"])],
                  text="The target snapshot/clone full path containing the new container disk files.",
                  required=True,
                  provisioning=True
                )

class KeywordSnapof(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="snapof",
                  depends=[('type', ["kvm", "xen", "ovm", "zone", "esx"])],
                  text="The snapshot origin full path containing the reference container disk files.",
                  required=True,
                  provisioning=True
                )

class KeywordContainerOrigin(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="container_origin",
                  depends=[('type', ["zone"])],
                  text="The origin container having the reference container disk files.",
                  required=True,
                  provisioning=True
                )

class KeywordRootfs(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="rootfs",
                  depends=[('type', ["lxc", "vz", "zone"])],
                  text="Sets the root fs directory of the container",
                  required=True,
                  provisioning=True
                )

class KeywordTemplate(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="template",
                  depends=[('type', ["lxc", "vz", "zone"])],
                  text="Sets the url of the template unpacked into the container root fs.",
                  required=True,
                  provisioning=True
                )

class KeywordVmName(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="name",
                  order=2,
                  depends=[('type', rcEnv.vt_supported)],
                  text="This need to be set if the virtual machine name is different from the service name."
                )

class KeywordSharedIpGroup(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="shared_ip_group",
                  order=11,
                  depends=[('type', rcEnv.vt_cloud)],
                  text="The cloud shared ip group name to allocate a public ip from."
                )

class KeywordSize(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="size",
                  order=11,
                  depends=[('type', rcEnv.vt_cloud)],
                  text="The cloud vm size, as known to the cloud manager. Example: tiny."
                )

class KeywordKeyName(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="key_name",
                  order=11,
                  depends=[('type', rcEnv.vt_cloud)],
                  text="The key name, as known to the cloud manager, to trust in the provisioned vm."
                )

class KeywordSrpPrmCores(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="prm_cores",
                  order=11,
                  depends=[('type', 'srp')],
                  default=1,
                  provisioning=True,
                  text="The number of core to bind the SRP container to."
                )

class KeywordSrpIp(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="ip",
                  order=11,
                  depends=[('type', 'srp')],
                  provisioning=True,
                  text="The ip name or addr used to create the SRP container."
                )

class KeywordSrpRootpath(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="rootpath",
                  order=11,
                  depends=[('type', 'srp')],
                  provisioning=True,
                  text="The path of the SRP container root filesystem."
                )

class KeywordCloudId(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="cloud_id",
                  order=11,
                  depends=[('type', rcEnv.vt_cloud)],
                  text="The cloud id as configured in node.conf. Example: cloud#1."
                )

class KeywordVmUuid(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="container",
                  keyword="uuid",
                  order=11,
                  depends=[('type', "ovm")],
                  text="The virtual machine unique identifier used to pass commands on the VM."
                )

class KeywordClusterType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="cluster_type",
                  order=15,
                  required=False,
                  default="failover",
                  candidates=["failover", "flex", "autoflex"],
                  text="failover: the service is allowed to be up on one node at a time. allactive: the service must be up on all nodes. flex: the service can be up on n out of m nodes (n <= m), n/m must be in the [flex_min_nodes, flex_max_nodes] range. autoflex: same as flex, but charge the collector to start the service on passive nodes when the average %cpu usage on active nodes > flex_cpu_high_threshold and stop the service on active nodes when the average %cpu usage on active nodes < flex_cpu_low_threshold."
                )

class KeywordFlexMinNodes(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="flex_min_nodes",
                  order=16,
                  required=False,
                  default=1,
                  depends=[('cluster_type', ['flex', 'autoflex'])],
                  text="Minimum number of active nodes in the cluster. Below this number alerts are raised by the collector, and the collector won't stop any more service instances."
                )

class KeywordFlexMaxNodes(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="flex_max_nodes",
                  order=16,
                  required=False,
                  default=10,
                  depends=[('cluster_type', ['flex', 'autoflex'])],
                  text="Maximum number of active nodes in the cluster. Above this number alerts are raised by the collector, and the collector won't start any more service instances. 0 means unlimited."
                )

class KeywordFlexCpuMinThreshold(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="flex_cpu_min_threshold",
                  order=16,
                  required=False,
                  default=10,
                  depends=[('cluster_type', ['flex', 'autoflex'])],
                  text="Average CPU usage across the active cluster nodes below which the collector raises alerts and decides to stop service instances with autoflex cluster type."
                )

class KeywordFlexCpuMaxThreshold(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="flex_cpu_max_threshold",
                  order=16,
                  required=False,
                  default=70,
                  depends=[('cluster_type', ['flex', 'autoflex'])],
                  text="Average CPU usage across the active cluster nodes above which the collector raises alerts and decides to start new service instances with autoflex cluster type."
                )

class KeywordServiceType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="service_type",
                  order=15,
                  required=True,
                  default=node_get_hostmode(),
                  candidates=rcEnv.allowed_svctype,
                  text="A non-PRD service can not be brought up on a PRD node, but a PRD service can be startup on a non-PRD node (in a DRP situation)."
                )

class KeywordNodes(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="nodes",
                  order=20,
                  required=True,
                  default=rcEnv.nodename,
                  text="List of cluster local nodes able to start the service.  Whitespace separated."
                )

class KeywordAutostartNode(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="autostart_node",
                  order=20,
                  required=True,
                  default=rcEnv.nodename,
                  text="A whitespace-separated list subset of 'nodes'. Defines the nodes where the service will try to start on upon node reboot. On a failover cluster there should only be one autostart node and the start-up will fail if the service is already up on another node though. If not specified, the service will never be started at node boot-time, which is rarely the expected behaviour."
                )

class KeywordDrpnode(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="drpnode",
                  order=21,
                  text="The backup node where the service is activated in a DRP situation. This node is also a data synchronization target for 'sync' resources."
                )

class KeywordDrpnodes(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="drpnodes",
                  order=21,
                  text="Alternate backup nodes, where the service could be activated in a DRP situation if the 'drpnode' is not available. These nodes are also data synchronization targets for 'sync' resources."
                )

class KeywordApp(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="app",
                  order=24,
                  default="DEFAULT",
                  text="Used to identify who is responsible for is service, who is billable and provides a most useful filtering key. Better keep it a short code."
                )

class KeywordComment(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="comment",
                  order=25,
                  text="Helps users understand the role of the service, which is nice to on-call support people having to operate on a service they are not usualy responsible for."
                )

class KeywordScsireserv(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="scsireserv",
                  order=25,
                  default=False,
                  candidates=(True, False),
                  text="If set to 'true', OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to disks of every disk group attached to this service. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to 'false' or not set, 'scsireserv' can be activated on a per-resource basis."
                )

class KeywordBwlimit(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="bwlimit",
                  order=25,
                  text="Bandwidth limit in KB applied to all rsync transfers. Leave empty to enforce no limit."
                )

class KeywordSyncInterval(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="sync_interval",
                  order=26,
                  default=121,
                  text="Set the minimum delay between syncs in minutes. If a sync is triggered through crond or manually, it is skipped if last sync occured less than 'sync_min_delay' ago. The mecanism is enforced by a timestamp created upon each sync completion in /opt/opensvc/var/sync/[service]![dst]"
                )

class KeywordSyncMaxDelay(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="sync_max_delay",
                  order=27,
                  default=1440,
                  text="Unit is minutes. This sets to delay above which the sync status of the resource is to be considered down. Should be set according to your application service level agreement. The cron job frequency should be set between 'sync_min_delay' and 'sync_max_delay'"
                )

class KeywordPresnapTrigger(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="presnap_trigger",
                  order=28,
                  text="Define a command to run before creating snapshots. This is most likely what you need to use plug a script to put you data in a coherent state (alter begin backup and the like)."
                )

class KeywordPostsnapTrigger(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="postsnap_trigger",
                  order=29,
                  text="Define a command to run after snapshots are created. This is most likely what you need to use plug a script to undo the actions of 'presnap_trigger'."
                )

class KeywordContainerize(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="DEFAULT",
                  keyword="containerize",
                  order=30,
                  default=True,
                  candidates=(True, False),
                  text="Use process containers when possible. Containers allow capping memory, swap and cpu usage per service. Lxc containers are naturally containerized, so skip containerization of their startapp."
                )

class KeywordContainerCpus(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="container_cpus",
                  order=31,
                  depends=[('containerize', [True])],
                  text="Allow service process to bind only the specified cpus. Cpus are specified as list or range : 0,1,2 or 0-2"
                )

class KeywordContainerMems(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="container_mems",
                  order=31,
                  depends=[('containerize', [True])],
                  text="Allow service process to bind only the specified memory nodes. Memory nodes are specified as list or range : 0,1,2 or 0-2"
                )

class KeywordContainerCpuShare(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="container_cpu_share",
                  order=31,
                  depends=[('containerize', [True])],
                  text="Kernel default value is used, which usually is 1024 shares. In a cpu-bound situation, ensure the service does not use more than its share of cpu ressource. The actual percentile depends on shares allowed to other services."
                )

class KeywordContainerMemLimit(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="container_mem_limit",
                  order=31,
                  depends=[('containerize', [True])],
                  text="Ensures the service does not use more than specified memory (in bytes). The Out-Of-Memory killer get triggered in case of tresspassing."
                )

class KeywordContainerVmemLimit(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="DEFAULT",
                  keyword="container_vmem_limit",
                  order=31,
                  depends=[('containerize', [True])],
                  text="Ensures the service does not use more than specified memory+swap (in bytes). The Out-Of-Memory killer get triggered in case of tresspassing. The specified value must be greater than container_mem_limit."
                )

class KeywordSyncType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="type",
                  order=10,
                  required=True,
                  candidates=("rsync", "dds", "netapp", "symsrdfs", "zfs", "symclone"),
                  default="rsync",
                  text="Point a sync driver to use."
                )

class KeywordSyncZfsSrc(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="src",
                  rtype="zfs",
                  order=10,
                  at=True,
                  required=True,
                  text="Source dataset of the sync."
                )

class KeywordSyncZfsDst(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="dst",
                  rtype="zfs",
                  order=11,
                  at=True,
                  required=True,
                  text="Destination dataset of the sync."
                )

class KeywordSyncZfsTarget(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="target",
                  rtype="zfs",
                  order=12,
                  required=True,
                  candidates=['nodes', 'drpnodes', 'nodes drpnodes'],
                  text="Describes which nodes should receive this data sync from the PRD node where the service is up and running. SAN storage shared 'nodes' must not be sync to 'nodes'. SRDF-like paired storage must not be sync to 'drpnodes'."
                )

class KeywordSyncZfsRecursive(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="recursive",
                  rtype="zfs",
                  order=13,
                  default=True,
                  candidates=(True, False),
                  text="Describes which nodes should receive this data sync from the PRD node where the service is up and running. SAN storage shared 'nodes' must not be sync to 'nodes'. SRDF-like paired storage must not be sync to 'drpnodes'."
                )

class KeywordSyncZfsTags(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="tags",
                  rtype="zfs",
                  text="The zfs sync resource supports the 'delay_snap' tag. This tag is used to delay the snapshot creation just before the sync, thus after 'postsnap_trigger' execution. The default behaviour (no tags) is to group all snapshots creation before copying data to remote nodes, thus between 'presnap_trigger' and 'postsnap_trigger'."
                )

class KeywordSyncRsyncSrc(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="src",
                  rtype="rsync",
                  order=10,
                  at=True,
                  required=True,
                  text="Source of the sync. Can be a whitespace-separated list of files or dirs passed as-is to rsync. Beware of the meaningful ending '/'. Refer to the rsync man page for details."
                )

class KeywordSyncRsyncDst(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="dst",
                  rtype="rsync",
                  order=11,
                  required=True,
                  text="Destination of the sync. Beware of the meaningful ending '/'. Refer to the rsync man page for details."
                )

class KeywordSyncRsyncTags(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="tags",
                  rtype="rsync",
                  text="The sync resource supports the 'delay_snap' tag. This tag is used to delay the snapshot creation just before the rsync, thus after 'postsnap_trigger' execution. The default behaviour (no tags) is to group all snapshots creation before copying data to remote nodes, thus between 'presnap_trigger' and 'postsnap_trigger'."
                )

class KeywordSyncRsyncExclude(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="exclude",
                  rtype="rsync",
                  text="!deprecated!. A whitespace-separated list of --exclude params passed unchanged to rsync. The 'options' keyword is preferred now."
                )

class KeywordSyncRsyncOptions(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="options",
                  rtype="rsync",
                  text="A whitespace-separated list of params passed unchanged to rsync. Typical usage is ACL preservation activation."
                )

class KeywordSyncRsyncTarget(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="target",
                  rtype="rsync",
                  order=12,
                  required=True,
                  candidates=['nodes', 'drpnodes', 'nodes drpnodes'],
                  text="Describes which nodes should receive this data sync from the PRD node where the service is up and running. SAN storage shared 'nodes' must not be sync to 'nodes'. SRDF-like paired storage must not be sync to 'drpnodes'."
                )

class KeywordSyncRsyncSnap(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="snap",
                  rtype="rsync",
                  order=14,
                  candidates=(True, False),
                  default=False,
                  text="If set to true, OpenSVC will try to snapshot the first snapshottable parent of the source of the sync and try to sync from the snap."
                )

class KeywordSyncRsyncDstfs(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="dstfs",
                  rtype="rsync",
                  order=13,
                  text="If set to a remote mount point, OpenSVC will verify that the specified mount point is really hosting a mounted FS. This can be used as a safety net to not overflow the parent FS (may be root)."
                )

class KeywordSyncRsyncBwlimit(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="sync",
                  keyword="bwlimit",
                  rtype="rsync",
                  text="Bandwidth limit in KB applied to this rsync transfer. Leave empty to enforce no limit. Takes precedence over 'bwlimit' set in [DEFAULT]."
                )

class KeywordSyncSyncInterval(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="sync",
                  keyword="sync_interval",
                  default=30,
                  text="Set the minimum delay between syncs in minutes. If a sync is triggered through crond or manually, it is skipped if last sync occured less than 'sync_min_delay' ago. If no set in a resource section, fallback to the value set in the 'default' section. The mecanism is enforced by a timestamp created upon each sync completion in /opt/opensvc/var/sync/[service]![dst]"
                )

class KeywordSyncSyncMaxDelay(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="sync",
                  keyword="sync_max_delay",
                  default=1440,
                  text="Unit is minutes. This sets to delay above which the sync status of the resource is to be considered down. Should be set according to your application service level agreement. The cron job frequency should be set between 'sync_min_delay' and 'sync_max_delay'."
                )

class KeywordIpIpname(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  keyword="ipname",
                  order=12,
                  at=True,
                  required=True,
                  text="The DNS name of the ip resource. Can be different from one node to the other, in which case '@nodename' can be specified. This is most useful to specify a different ip when the service starts in DRP mode, where subnets are likely to be different than those of the production datacenter."
                )

class KeywordIpZone(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  keyword="zone",
                  order=12,
                  at=True,
                  required=False,
                  text="The zone name the ip resource is linked to. If set, the ip is plumbed from the global in the zone context."
                )

class KeywordIpType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  keyword="type",
                  candidates=['crossbow'],
                  text="The opensvc ip driver name.",
                  required=False,
                  order=10
                )

class KeywordIpIpdev(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  keyword="ipdev",
                  order=11,
                  at=True,
                  required=True,
                  text="The interface name over which OpenSVC will try to stack the service ip. Can be different from one node to the other, in which case the '@nodename' can be specified."
                )

class KeywordIpIpdevext(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  rtype="crossbow",
                  keyword="ipdevext",
                  order=12,
                  at=True,
                  required=False,
                  default="v4",
                  text="The interface name extension for crossbow ipadm configuration."
                )

class KeywordIpNetmask(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  keyword="netmask",
                  order=13,
                  text="If an ip is already plumbed on the root interface (in which case the netmask is deduced from this ip). Mandatory if the interface is dedicated to the service (dummy interface are likely to be in this case). The format is decimal for IPv4, ex: 255.255.252.0, and octal for IPv6, ex: 64."
                )

class KeywordIpGateway(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="ip",
                  keyword="gateway",
                  order=14,
                  required=False,
                  text="A zone ip provisioning parameter used in the sysidcfg formatting. The format is decimal for IPv4, ex: 255.255.252.0, and octal for IPv6, ex: 64.",
                  provisioning=True
                )

class KeywordVgType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vg",
                  keyword="type",
                  order=9,
                  required=False,
                  candidates=['veritas'],
                  text="The volume group driver to use. Leave empty to activate the native volume group manager."
                )

class KeywordVgVgname(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vg",
                  keyword="vgname",
                  order=10,
                  required=True,
                  text="The name of the volume group"
                )

class KeywordVgOptions(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vg",
                  keyword="options",
                  default="",
                  required=False,
                  provisioning=True,
                  text="The vgcreate options to use upon vg provisioning."
                )

class KeywordVgDsf(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vg",
                  keyword="dsf",
                  candidates=(True, False),
                  default=True,
                  text="HP-UX only. 'dsf' must be set to false for LVM to use never-multipathed /dev/dsk/... devices. Otherwize, ad-hoc multipathed /dev/disk/... devices."
                )

class KeywordVgScsireserv(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vg",
                  keyword="scsireserv",
                  default=False,
                  candidates=(True, False),
                  text="If set to 'true', OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to disks of every disk group attached to this service. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to 'false' or not set, 'scsireserv' can be activated on a per-resource basis."
                )

class KeywordVgPvs(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vg",
                  keyword="pvs",
                  required=True,
                  text="The list of paths to the physical volumes of the volume group.",
                  provisioning=True
                )

class KeywordPoolPoolname(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="pool",
                  keyword="poolname",
                  order=10,
                  at=True,
                  text="The name of the zfs pool"
                )

class KeywordPoolTags(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="pool",
                  keyword="tags",
                  text=""
                )

class KeywordVmdgScsireserv(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vmdg",
                  keyword="scsireserv",
                  default=False,
                  candidates=(True, False),
                  text="If set to 'true', OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to disks of every disk group attached to this service. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to 'false' or not set, 'scsireserv' can be activated on a per-resource basis."
                )

class KeywordDrbdScsireserv(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="drbd",
                  keyword="scsireserv",
                  default=False,
                  candidates=(True, False),
                  text="If set to 'true', OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to disks of every disk group attached to this service. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to 'false' or not set, 'scsireserv' can be activated on a per-resource basis."
                )

class KeywordDrbdRes(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="drbd",
                  keyword="res",
                  order=11,
                  text="The name of the drbd resource associated with this service resource. OpenSVC expect the resource configuration file to reside in '/etc/drbd.d/resname.res'. The 'sync#i0' resource will take care of replicating this file to remote nodes."
                )

class KeywordFsDev(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="dev",
                  order=11,
                  at=True,
                  required=True,
                  text="The block device file or filesystem image file hosting the filesystem to mount. Different device can be set up on different nodes using the dev@nodename syntax"
                )

class KeywordFsZone(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="zone",
                  order=11,
                  at=True,
                  required=False,
                  text="The zone name the fs refers to. If set, the fs mount point is reparented into the zonepath rootfs."
                )

class KeywordFsVg(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="vg",
                  required=True,
                  text="The name of the disk group the filesystem device should be provisioned from.",
                  provisioning=True
                )

class KeywordFsSize(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="size",
                  required=True,
                  text="The size in MB of the logical volume to provision for this filesystem.",
                  provisioning=True
                )

class KeywordFsMnt(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="mnt",
                  order=12,
                  required=True,
                  text="The mount point where to mount the filesystem."
                )

class KeywordFsMntOpt(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="mnt_opt",
                  order=13,
                  text="The mount options."
                )

class KeywordFsType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="type",
                  order=14,
                  required=True,
                  text="The filesystem type. Used to determine the fsck command to use."
                )

class KeywordFsSnapSize(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="snap_size",
                  order=14,
                  text="If this filesystem is build on a snapable logical volume or is natively snapable (jfs, vxfs, ...) this setting overrides the default 10% of the filesystem size to compute the snapshot size. The snapshot is created by snap-enabled rsync-type sync resources. The unit is Megabytes."
                )

class KeywordFsTags(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="fs",
                  keyword="tags",
                  text="",
                  provisioning=True
                )


class KeywordLoopSize(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="loop",
                  keyword="size",
                  required=True,
                  default=10,
                  text="The size of the loop file to provision.",
                  provisioning=True
                )

class KeywordLoopFile(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="loop",
                  keyword="file",
                  required=True,
                  text="The file hosting the disk image to map."
                )

class KeywordSyncNetappFiler(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="filer",
                  rtype="netapp",
                  required=True,
                  at=True,
                  text="The Netapp filer resolvable host name used by the node.  Different filers can be set up for each node using the filer@nodename syntax."
                )

class KeywordSyncNetappPath(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="path",
                  rtype="netapp",
                  required=True,
                  text="Specifies the volume or qtree to drive snapmirror on."
                )

class KeywordSyncNetappUser(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="user",
                  rtype="netapp",
                  required=True,
                  default="nasadm",
                  text="Specifies the user used to ssh connect the filers. Nodes should be trusted by keys to access the filer with this user."
                )

class KeywordSyncSymsrdfsSymdg(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="symdg",
                  rtype="symsrdfs",
                  required=True,
                  text="Name of the symmetrix device group where the source and target SRDF devices are grouped."
                )

class KeywordSyncSymsrdfsRdfg(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="rdfg",
                  rtype="symsrdfs",
                  required=True,
                  text="Number of the symmetrix rdf group where the source and target SRDF/S devices are paired."
                )

class KeywordSyncSymcloneSymdg(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="symdg",
                  rtype="symclone",
                  required=True,
                  text="Name of the symmetrix device group where the source and target devices are grouped."
                )

class KeywordSyncSymclonePrecopyTimeout(KeywordInteger):
    def __init__(self):
        KeywordInteger.__init__(
                  self,
                  section="sync",
                  keyword="precopy_timeout",
                  rtype="symclone",
                  required=True,
                  default=300,
                  text="Seconds to wait for a precopy (syncresync) to finish before returning with an error. In this case, the precopy proceeds normally, but the opensvc leftover actions must be retried. The precopy time depends on the amount of changes logged at the source, which is context-dependent. Tune to your needs."
                )

class KeywordSyncSymcloneSymdevs(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="symdevs",
                  rtype="symclone",
                  required=True,
                  at=True,
                  default=300,
                  text="Whitespace-separated list of devices to drive with this resource. Devices are specified as 'symmetrix identifier:symmetrix device identifier. Different symdevs can be setup on each node using the symdevs@nodename."
                )

class KeywordSyncDdsSrc(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="src",
                  rtype="dds",
                  required=True,
                  text="Points the origin of the snapshots to replicate from."
                )

class KeywordSyncDdsDst(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="dst",
                  rtype="dds",
                  required=True,
                  text="Target file or block device. Optional. Defaults to src. Points the media to replay the binary-delta received from source node to. This media must have a size superior or equal to source."
                )

class KeywordSyncDdsTarget(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="target",
                  rtype="dds",
                  required=True,
                  candidates=['nodes', 'drpnodes', 'nodes drpnodes'],
                  text="Accepted values are 'drpnodes', 'nodes' or both, whitespace-separated. Points the target nodes to replay the binary-deltas on. Be warned that starting the service on a target node without a 'stop-syncupdate-start cycle, will break the synchronization, so this mode is usually restricted to drpnodes sync, and should not be used to replicate data between nodes with automated services failover."
                )

class KeywordSyncDdsSnapSize(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="sync",
                  keyword="snap_size",
                  rtype="dds",
                  text="Default to 10% of origin. In MB, rounded to physical extent boundaries by lvm tools. Size of the snapshots created by OpenSVC to extract binary deltas from. Opensvc creates at most 2 snapshots : one short-lived to gather changed data from, and one long-lived to gather changed chunks list from. Volume groups should have the necessary space always available."
                )

class KeywordVdiskPath(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="vdisk",
                  keyword="path",
                  required=True,
                  at=True,
                  text="Path of the device or file used as a virtual machine disk. The path@nodename can be used to to set up different path on each node."
                )

class KeywordHbType(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="hb",
                  keyword="type",
                  required=True,
                  candidates=('OpenHA', 'LinuxHA'),
                  text="Specify the heartbeat driver to use."
                )

class KeywordHbName(Keyword):
    def __init__(self):
        Keyword.__init__(
                  self,
                  section="hb",
                  keyword="name",
                  rtype="OpenHA",
                  text="Specify the service name used by the heartbeat. Defaults to the service name."
                )

class Section(object):
    def __init__(self, section):
        self.section = section
        self.keywords = []

    def __iadd__(self, o):
        if not isinstance(o, Keyword):
            return self
        self.keywords.append(o)
        return self

    def __str__(self):
        s = ''
        for keyword in sorted(self.keywords):
            s += str(keyword)
        return s

    def getkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None and not k.provisioning]
        else:
            return [k for k in self.keywords if k.rtype == rtype and not k.provisioning]

    def getprovkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None and k.provisioning]
        else:
            return [k for k in self.keywords if k.rtype == rtype and k.provisioning]

    def getkey(self, keyword, rtype=None):
        if '@' in keyword:
            l = keyword.split('@')
            if len(l) != 2:
                return None
            keyword, node = l
        for k in self.keywords:
            if k.keyword == keyword and k.rtype == rtype:
                return k
        return None

class KeywordStore(dict):
    def __init__(self, provision=False):
        self.sections = {}
        self.provision = provision

    def __iadd__(self, o):
        if not isinstance(o, Keyword):
            return self
        o.top = self
        if o.section not in self.sections:
             self.sections[o.section] = Section(o.section)
        self.sections[o.section] += o
        return self

    def __getattr__(self, key):
        return self.sections[str(key)]

    def __getitem__(self, key):
        return self.sections[str(key)]

    def __str__(self):
        s = ''
        for section in self.sections:
            s += str(self.sections[section])
        return s

    def required_keys(self, section, rtype=None):
        if section not in self.sections:
            return []
        return [k for k in sorted(self.sections[section].getkeys(rtype)) if k.required is True]

    def purge_keywords_from_dict(self, d, section):
        if 'type' in d:
            rtype = d['type']
        else:
            rtype = None
        for keyword, value in d.items():
            key = self.sections[section].getkey(keyword)
            if key is None and rtype is not None:
                key = self.sections[section].getkey(keyword, rtype)
            if key is None:
                if keyword != "rtype":
                    print("Remove unknown keyword '%s' from section '%s'"%(keyword, section))
                    del d[keyword]
        return d

    def update(self, rid, d):
        """ Given a resource dictionary, spot missing required keys
            and provide a new dictionary to merge populated by default
            values
        """
        import copy
        completion = copy.copy(d)

        # decompose rid into section and rtype
        if rid == 'DEFAULT':
            section = rid
            rtype = None
        else:
            if '#' not in rid:
                return {}
            l = rid.split('#')
            if len(l) != 2:
                return {}
            section = l[0]
            if 'type' in d:
                 rtype = d['type']
            elif self[section].getkey('type') is not None and \
                  self[section].getkey('type').default is not None:
                rtype = self[section].getkey('type').default
            else:
                rtype = None

        # validate command line dictionary
        for keyword, value in d.items():
            key = self.sections[section].getkey(keyword)
            if key is None and rtype is not None:
                key = self.sections[section].getkey(keyword, rtype)
            if key is None:
                continue
            if key.candidates is not None and value not in key.candidates:
                print("'%s' keyword has invalid value '%s' in section '%s'"%(keyword, str(value), rid))
                raise KeyInvalidValue()

        # add missing required keys if they have a known default value
        for key in self.required_keys(section, rtype):
            if key.keyword in d:
                continue
            if key.keyword in map(lambda x: x.split('@')[0], d.keys()):
                continue
            if key.default is None:
                sys.stderr.write("No default value for required key '%s' in section '%s'\n"%(key.keyword, rid))
                raise MissKeyNoDefault()
            print("Implicitely add [%s]"%rid, key.keyword, "=", key.default)
            completion[key.keyword] = key.default

        """
        # do we have a provisioning for for this resource ?
        prov = self.get_provisioning_class(section, completion)

        if prov is not None:
            # is provisioning needed ?
            tmp = {rid: completion}
            if prov(rid, tmp).validate():
                print(rid, "resource is valid")
            elif prov(rid, tmp).provisioner():
                print(rid, "resource provisioned")
            else:
                print(rid, "resource provisioning failed")
        """

        # purge unknown keywords and provisioning keywords
        completion = self.purge_keywords_from_dict(completion, section)

        return completion

    def form_sections(self, sections):
        from textwrap import TextWrapper
        wrapper = TextWrapper(subsequent_indent="%18s"%"", width=78)
        candidates = set(self.sections.keys()) - set(['DEFAULT'])

        print("------------------------------------------------------------------------------")
        print("Choose a resource type to add or a resource to edit.")
        print("Enter 'quit' to finish the creation.")
        print("------------------------------------------------------------------------------")
        print(wrapper.fill("resource types: "+', '.join(candidates)))
        print(wrapper.fill("resource ids:   "+', '.join(sections.keys())))
        print
        return raw_input("resource type or id> ")

    def free_resource_index(self, section, sections):
        indices = []
        for s in sections:
            l = s.split('#')
            if len(l) != 2:
                continue
            sname, sindex = l
            if section != sname:
                continue
            try:
                indices.append(int(sindex))
            except:
                continue
        i = 0
        while True:
            if i not in indices:
                return i
            i += 1

    def form(self, defaults, sections):
        for key in sorted(self.DEFAULT.getkeys()):
            defaults = key.form(defaults)
        while True:
            try:
                section = self.form_sections(sections)
            except EOFError:
                break
            if section == "quit":
                break
            if '#' in section:
                rid = section
                section = section.split('#')[0]
            else:
                index = self.free_resource_index(section, sections)
                rid = '#'.join((section, str(index)))
            if section not in self.sections:
                 print("unsupported resource type")
                 continue
            for key in sorted(self.sections[section].getkeys()):
                if rid not in sections:
                    sections[rid] = {}
                sections[rid] = key.form(sections[rid])
            if 'type' in sections[rid]:
                specific_keys = self.sections[section].getkeys(rtype=sections[rid]['type'])
                if len(specific_keys) > 0:
                    print("\nKeywords specific to the '%s' driver\n"%sections[rid]['type'])
                for key in sorted(specific_keys):
                    if rid not in sections:
                        sections[rid] = {}
                    sections[rid] = key.form(sections[rid])

            """
            # do we have a provisioning for for this resource ?
            prov = self.get_provisioning_class(section, sections[rid])

            if prov is None:
                continue

            # is provisioning needed ?
            if prov(rid, sections).validate():
                print(rid, "resource is valid")
                continue

            # toggle provisioning keywords
            # either --provision or user said so on prompt
            if not self.provision:
                tmp = {}
                tmp = KeywordProvision().form(tmp)
                if tmp['provision'] == "no":
                    continue

            provkeys = self.sections[section].getprovkeys()
            if len(provkeys) > 0:
                print("\nProvisioning keywords\n")
            for key in sorted(provkeys):
                if rid not in sections:
                    sections[rid] = {}
                sections[rid] = key.form(sections[rid])

            if 'type' in sections[rid]:
                specific_provkeys = self.sections[section].getprovkeys(rtype=sections[rid]['type'])
                if len(specific_provkeys) > 0:
                    print("\nProvisioning keywords specific to the '%s' driver\n"%sections[rid]['type'])
                for key in sorted(specific_provkeys):
                    if rid not in sections:
                        sections[rid] = {}
                    sections[rid] = key.form(sections[rid])

            # now we have everything needed to provision. just do it.
            prov(rid, sections).provisioner()
            """

            # purge the provisioning keywords
            sections[rid] = self.purge_keywords_from_dict(sections[rid], section)

        return defaults, sections

    """
    def get_provisioning_class(self, section, d):
        mod_prefix = 'prov'
        class_prefix = 'Provisioning'
        rtype = ""
        if section == 'DEFAULT':
            section = ""
            if 'mode' in d:
                rtype = d['mode']
        else:
            section = section[0].upper() + section[1:].lower()
            if 'type' in d:
                rtype = d['type']
        if len(rtype) > 2:
            rtype = rtype[0].upper() + rtype[1:].lower()
        try:
            m = __import__(mod_prefix+section+rtype)
            return getattr(m, class_prefix+section+rtype)
        except ImportError:
            import traceback
            traceback.print_exc()
            pass
        try:
            m = __import__(mod_prefix+section)
            return getattr(m, class_prefix+section)
        except ImportError:
            print(mod_prefix+section+rtype, "nor", mod_prefix+section, "provisioning modules not implemented")
            return None
    """

class KeyDict(KeywordStore):
    def __init__(self, provision=False):
        KeywordStore.__init__(self, provision)

        import os

        def kw_disable(resource):
            return Keyword(
                  section=resource,
                  keyword="disable",
                  candidates=(True, False),
                  default=False,
                  text="A disabled resource will be ignored on service startup and shutdown."
                )
        def kw_disable_on(resource):
            return Keyword(
                  section=resource,
                  keyword="disable_on",
                  text="A list of nodenames where to consider the 'disable' value is True. Also supports the 'nodes' and 'drpnodes' special values."
                )
        def kw_optional(resource):
            return Keyword(
                  section=resource,
                  keyword="optional",
                  candidates=(True, False),
                  default=False,
                  text="Possible values are 'true' or 'false'. Actions on resource will be tried upon service startup and shutdown, but action failures will be logged and passed over. Useful for resources like dump filesystems for example."
                )
        def kw_always_on(resource):
            return Keyword(
                  section=resource,
                  keyword="always_on",
                  candidates=['nodes', 'drpnodes', 'nodes drpnodes'],
                  text="Possible values are 'nodes', 'drpnodes' or 'nodes drpnodes', or a list of nodes. Sets the nodes on which the resource is always kept up. Primary usage is file synchronization receiving on non-shared disks. Don't set this on shared disk !! danger !!"
                )

        for r in ["sync", "ip", "fs", "vg", "hb", "pool", "vmdg", "drbd",
                  "loop", "vdisk"]:
            self += kw_disable(r)
            self += kw_disable_on(r)
            self += kw_optional(r)
            self += kw_always_on(r)

        self += KeywordMode()
        self += KeywordPkgName()
        self += KeywordDockerDataDir()
        self += KeywordClusterType()
        self += KeywordFlexMinNodes()
        self += KeywordFlexMaxNodes()
        self += KeywordFlexCpuMinThreshold()
        self += KeywordFlexCpuMaxThreshold()
        self += KeywordServiceType()
        self += KeywordNodes()
        self += KeywordAutostartNode()
        self += KeywordDrpnode()
        self += KeywordDrpnodes()
        self += KeywordApp()
        self += KeywordComment()
        self += KeywordScsireserv()
        self += KeywordBwlimit()
        self += KeywordSyncInterval()
        self += KeywordSyncMaxDelay()
        self += KeywordPresnapTrigger()
        self += KeywordPostsnapTrigger()
        self += KeywordContainerize()
        self += KeywordContainerCpus()
        self += KeywordContainerMems()
        self += KeywordContainerCpuShare()
        self += KeywordContainerMemLimit()
        self += KeywordContainerVmemLimit()
        self += KeywordSyncType()
        self += KeywordSyncZfsSrc()
        self += KeywordSyncZfsDst()
        self += KeywordSyncZfsTarget()
        self += KeywordSyncZfsRecursive()
        self += KeywordSyncZfsTags()
        self += KeywordSyncRsyncSrc()
        self += KeywordSyncRsyncDst()
        self += KeywordSyncRsyncTags()
        self += KeywordSyncRsyncExclude()
        self += KeywordSyncRsyncOptions()
        self += KeywordSyncRsyncTarget()
        self += KeywordSyncRsyncSnap()
        self += KeywordSyncRsyncDstfs()
        self += KeywordSyncRsyncBwlimit()
        self += KeywordSyncSyncInterval()
        self += KeywordSyncSyncMaxDelay()
        self += KeywordIpType()
        self += KeywordIpIpname()
        self += KeywordIpIpdev()
        self += KeywordIpIpdevext()
        self += KeywordIpNetmask()
        self += KeywordIpGateway()
        self += KeywordIpZone()
        self += KeywordVgType()
        self += KeywordVgVgname()
        self += KeywordVgDsf()
        self += KeywordVgOptions()
        self += KeywordVgScsireserv()
        self += KeywordVgPvs()
        self += KeywordPoolPoolname()
        self += KeywordPoolTags()
        self += KeywordVmdgScsireserv()
        self += KeywordDrbdScsireserv()
        self += KeywordDrbdRes()
        self += KeywordFsType()
        self += KeywordFsDev()
        self += KeywordFsZone()
        self += KeywordFsMnt()
        self += KeywordFsMntOpt()
        self += KeywordFsSnapSize()
        self += KeywordFsVg()
        self += KeywordFsSize()
        self += KeywordFsTags()
        self += KeywordLoopFile()
        self += KeywordLoopSize()
        self += KeywordSyncNetappFiler()
        self += KeywordSyncNetappPath()
        self += KeywordSyncNetappUser()
        self += KeywordSyncSymcloneSymdg()
        self += KeywordSyncSymclonePrecopyTimeout()
        self += KeywordSyncSymcloneSymdevs()
        self += KeywordSyncDdsSrc()
        self += KeywordSyncDdsDst()
        self += KeywordSyncDdsTarget()
        self += KeywordSyncDdsSnapSize()
        self += KeywordVdiskPath()
        self += KeywordHbType()
        self += KeywordHbName()
        self += KeywordContainerType()
        self += KeywordVmName()
        self += KeywordRootfs()
        self += KeywordTemplate()
        self += KeywordSharedIpGroup()
        self += KeywordSize()
        self += KeywordKeyName()
        self += KeywordCloudId()
        self += KeywordVmUuid()
        self += KeywordVirtinst()
        self += KeywordDockerRunCommand()
        self += KeywordDockerRunImage()
        self += KeywordDockerRunArgs()
        self += KeywordSnap()
        self += KeywordSnapof()
        self += KeywordContainerOrigin()
        self += KeywordSrpIp()
        self += KeywordSrpRootpath()
        self += KeywordSrpPrmCores()

if __name__ == "__main__":
    store = KeyDict()
    print(store)
    #print(store.DEFAULT.app)
    #print(store['DEFAULT'])
