import sys

from core.keywords import KeywordStore
from env import Env

SECTIONS = [
    "DEFAULT",
    "sync",
    "ip",
    "fs",
    "disk",
    "share",
    "container",
    "app",
    "task",
    "volume",
]

DATA_SECTIONS = [
    "certificate",
    "expose",
    "hashpolicy",
    "route",
    "vhost",
]

# deprecated => supported
DEPRECATED_KEYWORDS = {
    "DEFAULT.mode": None,
    "DEFAULT.cluster_type": "topology",
    "DEFAULT.service_type": "env",
    "DEFAULT.affinity": "hard_affinity",
    "DEFAULT.anti_affinity": "hard_anti_affinity",
    "DEFAULT.docker_data_dir": "container_data_dir",
    "DEFAULT.flex_min_nodes": "flex_min",
    "DEFAULT.flex_max_nodes": "flex_max",
    "always_on": None,
}

# supported => deprecated
REVERSE_DEPRECATED_KEYWORDS = {
    "DEFAULT.topology": "cluster_type",
    "DEFAULT.env": "service_type",
    "DEFAULT.hard_affinity": "affinity",
    "DEFAULT.hard_anti_affinity": "anti_affinity",
    "DEFAULT.container_data_dir": "docker_data_dir",
    "DEFAULT.flex_min": "flex_min_nodes",
    "DEFAULT.flex_max": "flex_max_nodes",
}

DEPRECATED_SECTIONS = {}

PG_KEYWORDS = [
    {
        "section": "DEFAULT",
        "keyword": "create_pg",
        "default": True,
        "convert": "boolean",
        "candidates": (True, False),
        "text": "Use process containers when possible. Containers allow capping memory, swap and cpu usage per service. Lxc containers are naturally containerized, so skip containerization of their startapp."
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_cpus",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "depends": [('create_pg', [True])],
        "text": "Allow service process to bind only the specified cpus. Cpus are specified as list or range : 0,1,2 or 0-2",
        "example": "0-2"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_mems",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "depends": [('create_pg', [True])],
        "text": "Allow service process to bind only the specified memory nodes. Memory nodes are specified as list or range : 0,1,2 or 0-2",
        "example": "0-2"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_cpu_shares",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "convert": "integer",
        "depends": [('create_pg', [True])],
        "text": "Kernel default value is used, which usually is 1024 shares. In a cpu-bound situation, ensure the service does not use more than its share of cpu ressource. The actual percentile depends on shares allowed to other services.",
        "example": "512"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_cpu_quota",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "depends": [('create_pg', [True])],
        "text": "The percent ratio of one core to allocate to the process group if % is specified, else the absolute value to set in the process group parameter. For example, on Linux cgroups, ``-1`` means unlimited, and a positive absolute value means the number of microseconds to allocate each period. ``50%@all`` means 50% of all cores, and ``50%@2`` means 50% of two cores.",
        "example": "50%@all"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_mem_oom_control",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "convert": "integer",
        "depends": [('create_pg', [True])],
        "text": "A flag (0 or 1) that enables or disables the Out of Memory killer for a cgroup. If enabled (0), tasks that attempt to consume more memory than they are allowed are immediately killed by the OOM killer. The OOM killer is enabled by default in every cgroup using the memory subsystem; to disable it, write 1.",
        "example": "1"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_mem_limit",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "convert": "size",
        "depends": [('create_pg', [True])],
        "text": "Ensures the service does not use more than specified memory (in bytes). The Out-Of-Memory killer get triggered in case of tresspassing.",
        "example": "512000000"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_vmem_limit",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "convert": "size",
        "depends": [('create_pg', [True])],
        "text": "Ensures the service does not use more than specified memory+swap (in bytes). The Out-Of-Memory killer get triggered in case of tresspassing. The specified value must be greater than :kw:`pg_mem_limit`.",
        "example": "1024000000"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_mem_swappiness",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "convert": "integer",
        "depends": [('create_pg', [True])],
        "text": "Set a swappiness value for the process group.",
        "example": "40"
    },
    {
        "sections": SECTIONS + ["subset"],
        "keyword": "pg_blkio_weight",
        "generic": True,
        "inheritance": "leaf",
        "at": True,
        "convert": "integer",
        "depends": [('create_pg', [True])],
        "text": "Block IO relative weight. Value: between 10 and 1000. Kernel default: 1000.",
        "example": "50"
    },
]

KEYWORDS = [
    {
        "section": "DEFAULT",
        "keyword": "id",
        "inheritance": "head",
        "default_text": "<random uuid>",
        "text": "A RFC 4122 random uuid generated by the agent. To use as reference in resources definitions instead of the service name, so the service can be renamed without affecting the resources."
    },
    {
        "section": "DEFAULT",
        "keyword": "priority",
        "default": Env.default_priority,
        "convert": "integer",
        "text": "A scheduling priority (0 for top priority) used by the monitor thread to trigger actions for the top priority services, so that the :kw:`node.max_parallel` constraint doesn't prevent high priority services to start first. The priority setting is dropped from a service configuration injected via the api by a user not granted the prioritizer role."
    },
    {
        "section": "DEFAULT",
        "keyword": "lock_timeout",
        "default": "60s",
        "convert": "duration",
        "text": "A duration expression, like ``1m30s``. The maximum wait time for the action lock acquire. The :cmd:`--waitlock` option overrides this parameter."
    },
    {
        "section": "DEFAULT",
        "keyword": "mode",
        "default": "hosted",
        "candidates": ["hosted"],
        "text": "Deprecated. The value is always ``hosted``. The keyword is kept around for now the ease transition from older agents."
    },
    {
        "section": "DEFAULT",
        "keyword": "rollback",
        "at": True,
        "default": True,
        "convert": "boolean",
        "text": "If set to ``false``, the default 'rollback on action error' "
                "behaviour is inhibited, leaving the service in its "
                "half-started state. The daemon also refuses to takeover "
                "a service if rollback is disabled and a peer instance is "
                "'start failed'."
    },
    {
        "section": "DEFAULT",
        "keyword": "comp_schedule",
        "at": True,
        "default": "00:00-06:00@361",
        "text": "The service compliance run schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "status_schedule",
        "at": True,
        "default": "@10",
        "text": "The service status evaluation schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "sync_schedule",
        "at": True,
        "default": "04:00-06:00@121",
        "text": "The default sync resources schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "run_schedule",
        "at": True,
        "text": "The default task resources schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "aws",
        "at": True,
        "text": "The aws cli executable fullpath. If not provided, aws is expected to be found in the PATH."
    },
    {
        "section": "DEFAULT",
        "keyword": "aws_profile",
        "at": True,
        "default": "default",
        "text": "The profile to use with the AWS api."
    },
    {
        "section": "DEFAULT",
        "keyword": "resinfo_schedule",
        "at": True,
        "default": "@60",
        "text": "The service resource info push schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "monitor_schedule",
        "at": True,
        "text": "The service resource monitor schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "push_schedule",
        "at": True,
        "default": "00:00-06:00@361",
        "text": "The service configuration emission to the collector schedule. See ``usr/share/doc/schedule`` for the schedule syntax."
    },
    {
        "section": "DEFAULT",
        "keyword": "flex_primary",
        "inheritance": "head",
        "convert": "lower",
        "at": True,
        "depends": [('topology', ["flex"])],
        "default_text": "<first node of the nodes parameter>",
        "text": "The node in charge of syncing the other nodes. :opt:`--cluster` actions on the flex_primary are executed on all peer nodes (ie, not drpnodes)."
    },
    {
        "section": "DEFAULT",
        "keyword": "drp_flex_primary",
        "inheritance": "head",
        "convert": "lower",
        "at": True,
        "depends": [('topology', ["flex"])],
        "default_text": "<first node of the drpnodes parameter>",
        "text": "The drpnode in charge of syncing the other drpnodes. :opt:`--cluster` actions on the drp_flex_primary are executed on all drpnodes (ie, not pri nodes)."
    },
    {
        "section": "DEFAULT",
        "keyword": "docker_exe",
        "at": True,
        "text": "If you have multiple docker versions installed and want the service to stick to a version whatever the ``PATH`` definition, you should set this parameter to the full path to the docker executable.",
        "example": "/usr/bin/docker-1.8"
    },
    {
        "section": "DEFAULT",
        "keyword": "dockerd_exe",
        "at": True,
        "text": "If you have multiple docker versions installed and want the service to stick to a version whatever the ``PATH`` definition, you should set this parameter to the full path to the docker daemon executable.",
        "example": "/usr/bin/dockerd-1.8"
    },
    {
        "section": "DEFAULT",
        "keyword": "container_data_dir",
        "at": True,
        "text": "If the service has lxc, docker or podman-type container resources and this keyword is set, the service configures a service-private containers data store. This setup is allows stateful service relocalization.",
        "example": "/srv/svc1/data/containers"
    },
    {
        "section": "DEFAULT",
        "keyword": "docker_daemon_private",
        "at": True,
        "default_text": "<true if container_data_dir is set, else false>",
        "convert": "boolean",
        "text": "If set to ``false``, this service will use the system's shared docker daemon instance. This is parameter is forced to ``false`` on non-Linux systems.",
        "example": "True"
    },
    {
        "section": "DEFAULT",
        "keyword": "docker_daemon_args",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "If the service has docker-type container resources, the service handles the startup of a private docker daemon. OpenSVC sets the socket and data dir parameters. Admins can set extra parameters using this keyword. For example, it can be useful to set the :opt:`--ip` parameter for a docker registry service.",
        "example": "--ip 1.2.3.4"
    },
    {
        "section": "DEFAULT",
        "keyword": "registry_creds",
        "at": True,
        "text": "The name of a secret in the same namespace having a config.json key which value is used to login to the container image registry. If not specified, the node-level registry credential store is used.",
        "example": "creds-registry-opensvc-com"
    },
    {
        "section": "DEFAULT",
        "keyword": "access",
        "inheritance": "head",
        "depends": [("kind", "vol")],
        "default": "rwo",
        "candidates": ["rwo", "roo", "rwx", "rox"],
        "at": True,
        "required": False,
        "text": "The access mode of the volume. ``rwo`` is Read Write Once, ``roo`` is Read Only Once, ``rwx`` is Read Write Many, ``rox`` is Read Only Many. ``rox`` and ``rwx`` modes are served by flex volume services.",
    },
    {
        "section": "DEFAULT",
        "keyword": "pool",
        "inheritance": "head",
        "depends": [("kind", "vol")],
        "at": True,
        "required": False,
        "text": "The name of the pool this volume was allocated from.",
    },
    {
        "section": "DEFAULT",
        "keyword": "size",
        "inheritance": "head",
        "depends": [("kind", "vol")],
        "convert": "integer",
        "at": True,
        "required": False,
        "text": "The size of the volume allocated from its pool.",
    },
    {
        "section": "subset",
        "keyword": "parallel",
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "If set to ``true``, actions are executed in parallel amongst the subset member resources.",
    },
    {
        "section": "container",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "candidates": [],
        "text": "The type of container.",
        "required": False,
        "default": "oci",
    },
    {
        "section": "DEFAULT",
        "keyword": "hard_affinity",
        "inheritance": "head",
        "convert": "set",
        "default": set(),
        "at": True,
        "text": "A whitespace separated list of services that must be started on the node to allow the monitor to start this service.",
        "example": "svc1 svc2"
    },
    {
        "section": "DEFAULT",
        "keyword": "hard_anti_affinity",
        "inheritance": "head",
        "convert": "set",
        "default": set(),
        "at": True,
        "text": "A whitespace separated list of services that must not be started on the node to allow the monitor to start this service.",
        "example": "svc1 svc2"
    },
    {
        "section": "DEFAULT",
        "keyword": "soft_affinity",
        "inheritance": "head",
        "convert": "set",
        "default": set(),
        "at": True,
        "text": "A whitespace separated list of services that must be started on the node to allow the monitor to start this service. If the local node is the only candidate ignore this constraint and allow start.",
        "example": "svc1 svc2"
    },
    {
        "section": "DEFAULT",
        "keyword": "soft_anti_affinity",
        "inheritance": "head",
        "convert": "set",
        "default": set(),
        "at": True,
        "text": "A whitespace separated list of services that must not be started on the node to allow the monitor to start this service. If the local node is the only candidate ignore this constraint and allow start.",
        "example": "svc1 svc2"
    },
    {
        "section": "DEFAULT",
        "keyword": "prkey",
        "at": True,
        "text": "Defines a specific default persistent reservation key for the service. A prkey set in a resource takes priority. If no prkey is specified in the service nor in the ``DEFAULT`` section, the prkey in ``node.conf`` is used. If ``node.conf`` has no prkey set, the hostid is computed and written in ``node.conf``."
    },
    {
        "section": "DEFAULT",
        "keyword": "no_preempt_abort",
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "If set to ``true``, OpenSVC will preempt scsi reservation with a preempt command instead of a preempt and and abort. Some scsi target implementations do not support this last mode (esx). If set to ``false`` or not set, :kw:`no_preempt_abort` can be activated on a per-resource basis."
    },
    {
        "section": "DEFAULT",
        "keyword": "show_disabled",
        "inheritance": "head",
        "at": True,
        "default": True,
        "convert": "boolean",
        "candidates": [True, False],
        "text": "Specifies if the disabled resources must be included in the print status and json status output."
    },
    {
        "section": "DEFAULT",
        "keyword": "topology",
        "inheritance": "head",
        "at": True,
        "default": "failover",
        "candidates": ["failover", "flex"],
        "text": "``failover`` the service is allowed to be up on one node at a time. ``flex`` the service can be up on :kw:`flex_target` nodes, where :kw:`flex_target` must be in the [flex_min, flex_max] range."
    },
    {
        "section": "DEFAULT",
        "keyword": "scale",
        "inheritance": "head",
        "at": True,
        "convert": "integer",
        "text": "If set, create and provision the necessary slave services, named ``<n>.<name>``, to meet the target ``<scale>`` number of started instances.",
        "example": "4"
    },
    {
        "section": "DEFAULT",
        "keyword": "scaler_slave",
        "inheritance": "head",
        "convert": "boolean",
        "default": False,
        "at": True,
        "text": "Automatically set to ``true`` by the daemon monitor when creating new scaler slaves."
    },
    {
        "section": "DEFAULT",
        "keyword": "orchestrate",
        "inheritance": "head",
        "at": True,
        "default": "no",
        "convert": "string",
        "candidates": ("ha", "start", "no"),
        "text": "If set to ``no``, disable service orchestration by the OpenSVC daemon monitor, including service start on boot. If set to ``start`` failover services won't failover automatically, though the service instance on the natural placement leader is started if another instance is not already up. Flex services won't restart the :kw:`flex_target` number of up instances. Resource restart is still active whatever the :kw:`orchestrate` value.",
    },
    {
        "section": "DEFAULT",
        "keyword": "stonith",
        "inheritance": "head",
        "convert": "boolean",
        "default": False,
        "candidates": (True, False),
        "depends": [("topology", ["failover"])],
        "text": "Stonith the node previously running the service if stale upon start by the daemon monitor.",
    },
    {
        "section": "DEFAULT",
        "keyword": "placement",
        "inheritance": "head",
        "default": "nodes order",
        "candidates": ["none", "nodes order", "load avg", "shift", "spread", "score"],
        "text": "Set a service instances placement policy:\n\n"
                       "* ``none`` no placement policy. a policy for dummy, observe-only, services.\n"
                       "* ``nodes order`` the left-most available node is allowed to start a service instance when necessary.\n"
                       "* ``load avg`` the least loaded node takes precedences.\n"
                       "* ``shift`` shift the nodes order ranking by the service prefix converter to an integer.\n"
                       "* ``spread`` a spread policy tends to perfect leveling with many services.\n"
                       "* ``score`` the highest scoring node takes precedence (the score is a composite indice of load, mem and swap).\n",
    },
    {
        "section": "DEFAULT",
        "keyword": "constraints",
        "inheritance": "head",
        "at": True,
        "depends": [("orchestrate", "ha")],
        "example": "$(\"{nodename}\"==\"n2.opensvc.com\")",
        "text": "An expression evaluating as a boolean, constraining the service instance placement by the daemon monitor to nodes with the constraints evaluated as True.\n\nThe constraints are not honored by manual start operations. The constraints value is embedded in the json status.\n\nSupported comparison operators are ``==``, ``!=``, ``>``, ``>=``, ``<=``, ``in (e1, e2)``, ``in [e1, e2]``.\n\nSupported arithmetic operators are ``*``, ``+``, ``-``, ``/``, ``**``, ``//``, ``%``.\n\nSupported binary operators are ``&``, ``|``, ``^``.\n\nThe negation operator is ``not``.\n\nSupported boolean operators are ``and``, ``or``.\n\nReferences are allowed.\n\nStrings, and references evaluating as strings, containing dots must be quoted.",
    },
    {
        "section": "DEFAULT",
        "keyword": "flex_min",
        "inheritance": "head",
        "default": 1,
        "convert": "integer",
        "depends": [("topology", ["flex"])],
        "text": "Minimum number of up instances in the cluster. Below this number the aggregated service status is degraded to warn.."
    },
    {
        "section": "DEFAULT",
        "keyword": "flex_max",
        "inheritance": "head",
        "default_text": "<number of svc nodes>",
        "convert": "integer",
        "depends": [("topology", ["flex"])],
        "text": "Maximum number of up instances in the cluster. Above this number the aggregated service status is degraded to warn. ``0`` means unlimited."
    },
    {
        "section": "DEFAULT",
        "keyword": "flex_target",
        "inheritance": "head",
        "default_text": "<the value of flex_min>",
        "convert": "integer",
        "depends": [("topology", ["flex"])],
        "text": "Optimal number of up instances in the cluster. The value must be between :kw:`flex_min` and :kw:`flex_max`. If ``orchestrate=ha``, the monitor ensures the :kw:`flex_target` is met."
    },
    {
        "section": "DEFAULT",
        "keyword": "flex_cpu_low_threshold",
        "inheritance": "head",
        "default": 0,
        "convert": "integer",
        "depends": [("topology", ["flex"])],
        "text": "Cluster-wide load average below which flex service instances will be stopped.",
    },
    {
        "section": "DEFAULT",
        "keyword": "flex_cpu_high_threshold",
        "inheritance": "head",
        "default": 100,
        "convert": "integer",
        "depends": [("topology", ["flex"])],
        "text": "Cluster-wide load average above which flex new service instances will be started.",
    },
    {
        "section": "DEFAULT",
        "keyword": "env",
        "inheritance": "head",
        "default_text": "<same as node env>",
        "candidates": Env.allowed_svc_envs,
        "text": "A non-PRD service can not be brought up on a PRD node, but a PRD service can be startup on a non-PRD node (in a DRP situation). The default value is the node :kw:`env`."
    },
    {
        "section": "DEFAULT",
        "keyword": "parents",
        "inheritance": "head",
        "at": True,
        "default": [],
        "default_text": "",
        "convert": "list_lower",
        "text": "List of services or instances expressed as ``<path>[@<nodename>]`` that must be ``avail up`` before allowing this service to be started by the daemon monitor. Whitespace separated."
    },
    {
        "section": "DEFAULT",
        "keyword": "children",
        "inheritance": "head",
        "at": True,
        "default": [],
        "default_text": "",
        "convert": "list_lower",
        "text": "List of services that must be ``avail down`` before allowing this service to be stopped by the daemon monitor. Whitespace separated."
    },
    {
        "section": "DEFAULT",
        "keyword": "slaves",
        "inheritance": "head",
        "at": True,
        "default": [],
        "convert": "list",
        "text": "List of services to propagate the :c-action:`start` and :c-action:`stop` actions to."
    },
    {
        "section": "DEFAULT",
        "keyword": "nodes",
        "inheritance": "head",
        "at": True,
        "convert": "nodes_selector",
        "default": Env.nodename,
        "default_text": "<hostname of the current node>",
        "text": "A node selector expression specifying the list of cluster nodes hosting service instances."
    },
    {
        "section": "DEFAULT",
        "keyword": "drpnode",
        "inheritance": "head",
        "convert": "lower",
        "default": "",
        "at": True,
        "text": "The backup node where the service is activated in a DRP situation. This node is also a data synchronization target for :c-res:`sync` resources.",
        "example": "node1"
    },
    {
        "section": "DEFAULT",
        "keyword": "drpnodes",
        "inheritance": "head",
        "at": True,
        "convert": "list_lower",
        "default": [],
        "default_text": "",
        "text": "Alternate backup nodes, where the service could be activated in a DRP situation if the 'drpnode' is not available. These nodes are also data synchronization targets for :c-res:`sync` resources.",
        "example": "node1 node2"
    },
    {
        "section": "DEFAULT",
        "keyword": "encapnodes",
        "inheritance": "head",
        "convert": "list_lower",
        "default": [],
        "default_text": "",
        "text": "The list of `containers` handled by this service and with an OpenSVC agent installed to handle the encapsulated resources. With this parameter set, parameters can be scoped with the ``@encapnodes`` suffix.",
        "example": "vm1 vm2"
    },
    {
        "section": "DEFAULT",
        "keyword": "app",
        "default": "default",
        "text": "Used to identify who is responsible for this service, who is billable and provides a most useful filtering key. Better keep it a short code."
    },
    {
        "section": "DEFAULT",
        "keyword": "scsireserv",
        "at": True,
        "default": False,
        "convert": "boolean",
        "candidates": (True, False),
        "text": "If set to ``true``, OpenSVC will try to acquire a type-5 (write exclusive, registrant only) scsi3 persistent reservation on every path to disks of every disk group attached to this service. Existing reservations are preempted to not block service start-up. If the start-up was not legitimate the data are still protected from being written over from both nodes. If set to ``false`` or not set, :kw:`scsireserv` can be activated on a per-resource basis."
    },
    {
        "section": "DEFAULT",
        "keyword": "bwlimit",
        "convert": "speed_kps",
        "text": "Bandwidth limit in KB applied to all rsync transfers. Leave empty to enforce no limit.",
        "example": "3 mb/s"
    },
    {
        "section": "DEFAULT",
        "keyword": "sync_interval",
        "default": 121,
        "convert": "duration",
        "text": "Set the minimum delay between syncs in minutes. If a sync is triggered through a scheduler or manually, it is skipped if last sync occurred less than :kw:`sync_min_delay` ago. The mecanism is enforced by a timestamp created upon each sync completion in ``<pathvar>/services/<namespace>/<kind>/<name>/<rid>/last_sync_<node>``"
    },
    {
        "section": "DEFAULT",
        "keyword": "sync_max_delay",
        "default": "1d",
        "convert": "duration_minute",
        "text": "Unit is minutes. This sets to delay above which the sync status of the resource is to be considered down. Should be set according to your application service level agreement. The scheduler task frequency should be set between :kw:`sync_min_delay` and :kw:`sync_max_delay`."
    },
    {
        "section": "DEFAULT",
        "keyword": "presnap_trigger",
        "convert": "shlex",
        "text": "Define a command to run before creating snapshots. This is most likely what you need to use plug a script to put you data in a coherent state (alter begin backup and the like).",
        "example": "/srv/svc1/etc/init.d/pre_snap.sh"
    },
    {
        "section": "DEFAULT",
        "keyword": "postsnap_trigger",
        "convert": "shlex",
        "text": "Define a command to run after snapshots are created. This is most likely what you need to use plug a script to undo the actions of :kw:`presnap_trigger`.",
        "example": "/srv/svc1/etc/init.d/post_snap.sh"
    },
    {
        "section": "DEFAULT",
        "keyword": "monitor_action",
        "at": True,
        "candidates": ("reboot", "crash", "freezestop", "switch"),
        "text": "The action to take when a monitored resource is not up nor standby up, and if the resource restart procedure has failed.",
        "example": "reboot"
    },
    {
        "section": "DEFAULT",
        "keyword": "pre_monitor_action",
        "at": True,
        "text": "A script to execute before the :kw:`monitor_action`. For example, if the :kw:`monitor_action` is set to ``freezestop``, the script can decide to crash the server if it detects a situation were the freezestop can not succeed (ex. fs can not be umounted with a dead storage array).",
        "example": "/bin/true"
    },
    {
        "section": "app",
        "keyword": "type",
        "inheritance": "leaf",
        "candidates": [],
        "default": "forking",
        "text": "The app driver to use. ``simple`` for foreground-running apps. ``forking`` for daemonizing apps."
    },
    {
        "section": "sync",
        "keyword": "type",
        "inheritance": "leaf",
        "candidates": [],
        "default": "rsync",
        "text": "Point a sync driver to use."
    },
    {
        "section": "sync",
        "keyword": "schedule",
        "default_keyword": "sync_schedule",
        "at": True,
        "text": "Set the this resource synchronization schedule. See ``/usr/share/doc/opensvc/schedule`` for the schedule syntax reference.",
        "example": '["00:00-01:00@61 mon", "02:00-03:00@61 tue-sun"]'
    },
    {
        "section": "sync",
        "keyword": "sync_max_delay",
        "default": "1d",
        "convert": "duration_minute",
        "text": "Unit is minutes. This sets to delay above which the sync status of the resource is to be considered down. Should be set according to your application service level agreement. The scheduler task frequency should be set between :kw:`sync_min_delay` and :kw:`sync_max_delay`."
    },
    {
        "section": "ip",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "default": "host",
        "candidates": [],
        "text": "The opensvc ip driver name.",
        "example": "crossbow",
    },
    {
        "section": "disk",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "default": "vg",
        "candidates": [],
        "text": "The volume group driver to use. Leave empty to activate the native volume group manager."
    },
    {
        "section": "fs",
        "keyword": "type",
        "protoname": "fs_type",
        "inheritance": "leaf",
        "at": True,
        "required": True,
        "strict_candidates": False,
        "candidates": [],
        "text": "The filesystem type for the generic driver or the fs driver."
    },
    {
        "section": "share",
        "keyword": "type",
        "inheritance": "leaf",
        "candidates": [],
        "text": "The type of share.",
        "required": True,
    },
    {
        "section": "task",
        "keyword": "type",
        "candidates": [],
        "default": "host",
        "text": "The type of task. Default tasks run on the host, their use is limited to the cluster admin population. Containerized tasks are safe for unprivileged population."
    },
    {
        "section": "expose",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "candidates": [],
        "text": "The type of expose.",
        "default": "envoy",
    },
    {
        "section": "vhost",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "candidates": [],
        "text": "The type of vhost.",
        "default": "envoy",
    },
    {
        "section": "route",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "candidates": [],
        "text": "The type of route.",
        "default": "envoy",
    },
    {
        "section": "hashpolicy",
        "keyword": "type",
        "inheritance": "leaf",
        "candidates": [],
        "text": "The type of hash policy.",
        "default": "envoy",
    },
    {
        "section": "certificate",
        "keyword": "type",
        "inheritance": "leaf",
        "at": True,
        "candidates": [],
        "text": "The type of certificate.",
        "default": "tls",
    },
    {
        "sections": SECTIONS,
        "keyword": "tags",
        "convert": "set",
        "generic": True,
        "at": True,
        "candidates": None,
        "default": set(),
        "default_text": "",
        "example": "encap noaction",
        "text": "A list of tags. Arbitrary tags can be used to limit action scope to resources with a specific tag. Some tags can influence the driver behaviour. For example :c-tag:`noaction` avoids any state changing action from the driver and implies ``optional=true``, :c-tag:`nostatus` forces the status to n/a."
    },
    {
        "sections": SECTIONS,
        "keyword": "subset",
        "inheritance": "leaf",
        "generic": True,
        "at": True,
        "text": "Assign the resource to a specific subset."
    },
    {
        "sections": SECTIONS,
        "keyword": "restart",
        "generic": True,
        "at": True,
        "default": 0,
        "convert": "integer",
        "text": "The agent will try to restart a resource <n> times before falling back to the monitor action. "
                "A resource restart is triggered if :"
                "the resource is not disabled and its status is not up, "
                "and the node is not frozen, "
                "and the service instance is not frozen "
                "and its local expect is set to ``started``. "
                "If a resource has a restart set to a value >0, its status is evaluated "
                "at the frequency defined by :kw:`DEFAULT.monitor_schedule` "
                "instead of the frequency defined by :kw:`DEFAULT.status_schedule`. "
                "Standby resources have a particular value to ensure best effort to restart standby resources, "
                "default value is 2, and value lower than 2 are changed to 2."
    },
    {
        "sections": SECTIONS,
        "keyword": "provision",
        "protoname": "enable_provision",
        "generic": True,
        "at": True,
        "candidates": (True, False),
        "default": True,
        "convert": "boolean",
        "text": "Set to false to skip the resource on provision and unprovision actions. Warning: Provision implies destructive operations like formating. Unprovision destroys service data."
    },
    {
        "sections": SECTIONS,
        "keyword": "unprovision",
        "protoname": "enable_unprovision",
        "generic": True,
        "at": True,
        "candidates": (True, False),
        "default": True,
        "convert": "boolean",
        "text": "Set to false to skip the resource on unprovision actions. Warning: Unprovision destroys service data."
    },
    {
        "sections": SECTIONS,
        "keyword": "shared",
        "generic": True,
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "Set to ``true`` to skip the resource on provision and unprovision actions if the action has already been done by a peer. Shared resources, like vg built on SAN disks must be provisioned once. All resources depending on a shared resource must also be flagged as shared."
    },
    {
        "sections": SECTIONS,
        "keyword": "encap",
        "generic": True,
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "Set to ``true`` to ignore this resource in the nodes context and consider it in the encapnodes context. The resource is thus handled by the agents deployed in the service containers."
    },
    {
        "sections": SECTIONS,
        "keyword": "monitor",
        "generic": True,
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "A down monitored resource will trigger a node suicide if the monitor thinks it should be up and the resource can not be restarted."
    },
    {
        "sections": SECTIONS,
        "keyword": "disable",
        "protoname": "disabled",
        "inheritance": "leaf",
        "generic": True,
        "at": True,
        "candidates": (True, False),
        "default": False,
        "convert": "boolean",
        "text": "A disabled resource will be ignored on service startup and shutdown. Its status is always reported ``n/a``.\n\nSet in DEFAULT, the whole service is disabled. A disabled service does not honor :c-action:`start` and :c-action:`stop` actions. These actions immediately return success.\n\n:cmd:`om <path> disable` only sets :kw:`DEFAULT.disable`. As resources disabled state is not changed, :cmd:`om <path> enable` does not enable disabled resources."
    },
    {
        "sections": SECTIONS,
        "keyword": "optional",
        "generic": True,
        "at": True,
        "convert": "tristate",
        "default_text": "true for tasks, syncs and resources tagged 'noaction', else false",
        "text": "Action failures on optional resources are logged but do not stop the action sequence. Also the optional resource status is not aggregated to the instance 'availstatus', but aggregated to the 'overallstatus'. Resource tagged :c-tag:`noaction` and sync resources are automatically considered optional. Useful for resources like dump filesystems for example."
    },
    {
        "sections": SECTIONS,
        "keyword": "standby",
        "generic": True,
        "at": True,
        "convert": "tristate",
        "text": "Always start the resource, even on standby instances. The daemon is responsible for starting standby resources. A resource can be set standby on a subset of nodes using keyword scoping.\n\nA typical use-case is sync'ed fs on non-shared disks: the remote fs must be mounted to not overflow the underlying fs.\n\n.. warning:: Don't set shared resources standby: fs on shared disks for example."
    },
    {
        "sections": SECTIONS,
        "keyword": "always_on",
        "generic": True,
        "convert": "list",
        "default": [],
        "default_text": "",
        "candidates": ['nodes', 'drpnodes'],
        "strict_candidates": False,
        "text": "Possible values are ``nodes``, ``drpnodes`` or ``nodes drpnodes``, or a list of nodes. Sets the nodes on which the resource is always kept up. Primary usage is file synchronization receiving on non-shared disks. Don't set this on shared disk !! danger !!"
    },
    {
        "sections": SECTIONS,
        "keyword": "pre_unprovision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`unprovision` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "post_unprovision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`unprovision` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "pre_provision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`provision` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "post_provision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`provision` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "pre_start",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`start` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "post_start",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`start` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "pre_startstandby",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`startstandby` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "post_startstandby",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`startstandby` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_pre_startstandby",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`startstandby` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_post_startstandby",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`startstandby` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "pre_stop",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`stop` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "post_stop",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`stop` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "pre_sync_nodes",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_nodes` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "post_sync_nodes",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_nodes` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "pre_sync_drp",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_drp` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "post_sync_drp",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_drp` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "pre_sync_restore",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_restore` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "post_sync_restore",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_restore` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "pre_sync_resync",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_resync` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "post_sync_resync",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_resync` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "pre_sync_update",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_update` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "post_sync_update",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_update` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT"],
        "keyword": "pre_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`run` action. Errors do not interrupt the action."
    },
    {
        "sections": ["DEFAULT"],
        "keyword": "post_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`run` action. Errors do not interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_pre_unprovision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`unprovision` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_post_unprovision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`unprovision` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_pre_provision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`provision` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_post_provision",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`provision` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_pre_start",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`start` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_post_start",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`start` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_pre_stop",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`stop` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "keyword": "blocking_post_stop",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`stop` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_pre_sync_nodes",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_nodes` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_post_sync_nodes",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_nodes` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_pre_sync_drp",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_drp` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_post_sync_drp",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_drp` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_pre_sync_restore",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_restore` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_post_sync_restore",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_restore` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_pre_sync_resync",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_resync` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_post_sync_resync",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_resync` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_pre_sync_update",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`sync_update` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "keyword": "blocking_post_sync_update",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`sync_update` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT"],
        "keyword": "blocking_pre_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute before the resource :c-action:`run` action. Errors interrupt the action."
    },
    {
        "sections": ["DEFAULT"],
        "keyword": "blocking_post_run",
        "generic": True,
        "at": True,
        "text": "A command or script to execute after the resource :c-action:`run` action. Errors interrupt the action."
    },
    {
        "sections": SECTIONS,
        "prefixes": ["unprovision", "provision", "start", "stop"],
        "keyword": "_requires",
        "generic": True,
        "at": True,
        "example": "ip#0 fs#0(down,stdby down)",
        "default": "",
        "text": "A whitespace-separated list of conditions to meet to accept running a '{prefix}' action. A condition is expressed as ``<rid>(<state>,...)``. If states are omitted, ``up,stdby up`` is used as the default expected states."
    },
    {
        "section": "DEFAULT",
        "keyword": "stat_timeout",
        "convert": "duration",
        "default": 5,
        "at": True,
        "text": "The maximum wait time for a stat call to respond. When expired, the resource status is degraded is to warn, which might cause a TOC if the resource is monitored."
    },
    {
        "sections": ["DEFAULT"],
        "prefixes": ["run"],
        "keyword": "_requires",
        "generic": True,
        "at": True,
        "example": "ip#0 fs#0(down,stdby down)",
        "default": "",
        "text": "A whitespace-separated list of conditions to meet to accept running a '{prefix}' action. A condition is expressed as ``<rid>(<state>,...)``. If states are omitted, ``up,stdby up`` is used as the default expected states."
    },
    {
        "sections": ["DEFAULT", "sync"],
        "prefixes": ["sync_nodes", "sync_drp", "sync_update", "sync_break", "sync_resync", "sync_restore"],
        "keyword": "_requires",
        "generic": True,
        "at": True,
        "example": "ip#0 fs#0(down,stdby down)",
        "default": "",
        "text": "A whitespace-separated list of conditions to meet to accept running a '{prefix}' action. A condition is expressed as ``<rid>(<state>,...)``. If states are omitted, ``up,stdby up`` is used as the default expected states."
    },
    {
        "sections": DATA_SECTIONS + SECTIONS + ["subset"],
        "keyword": "comment",
        "default": "",
        "text": "Helps users understand the role of the service and resources, which is nice to on-call support people having to operate on a service they are not usually responsible for."
    },
] + PG_KEYWORDS


KEYS = KeywordStore(
    name="svc",
    provision=True,
    keywords=KEYWORDS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
    base_sections=["env", "DEFAULT"],
    template_prefix="template.service.",
)
