import os
import re

from subprocess import *

import exceptions as ex
import rcStatus

from .. import BaseDisk, BASE_KEYWORDS
from svcBuilder import init_kwargs
from core.objects.svcdict import KEYS

DRIVER_GROUP = "disk"
DRIVER_BASENAME = "ldom"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "container_id",
        "required": True,
        "at": True,
        "text": "The id of the container whose configuration to extract the disk mapping from."
    },
]
DEPRECATED_SECTIONS = {
    "vmdg": ["disk", "ldom"],
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_sections=DEPRECATED_SECTIONS,
)

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["container_id"] = svc.oget(s, "container_id")

    if not kwargs["container_id"] in svc.cd:
        svc.log.error("%s.container_id points to an invalid section"%kwargs["container_id"])
        return

    try:
        container_type = svc.conf_get(kwargs["container_id"], "type")
    except ex.OptNotFound as exc:
        svc.log.error("type must be set in section %s"%kwargs["container_id"])
        return

    if container_type != "ldom":
        return

    r = DiskLdom(**kwargs)
    svc += r


class DiskLdom(BaseDisk):
    def __init__(self, container_id=None, **kwargs):
        super(DiskLdom, self).__init__(type='disk.ldom', **kwargs)
        self.label = "vmdg %s" % self.name
        self.container_id = container_id

    def has_it(self):
        return True

    def is_up(self):
        return True

    def _status(self, verbose=False):
        return rcStatus.NA

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def sub_devs(self):
        return self.exposed_devs()

    def exposed_devs(self):
        """
            VCC|name=vccname|...
            VDS|name=vdsname|...
            |vol=volname|..|dev=/dev/...|....
            |vol=volname1|..|dev=/dev/...|....
            VDS|name=vdsname1|..
            |vol=volname2|..|dev=/dev/...|....

            ldm list -o disk -p domname
            VERSION
            DOMAIN|..
            VDISK|name=...|vol=volname@vds|...
            VDISK|name=...|vol=volname2@vds2|...
        """
        vdevname2dev = {}
        devs = set()

        cmd = [ '/usr/sbin/ldm', 'list-services' , '-p' ]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            raise ex.excError
        vds = ''
        for line in buff[0].split('\n'):
            keys = line.split('|')
            if keys[0] == 'VDS' and len(keys) > 1 :
                for k in keys :
                    name_value = k.split('=')
                    if name_value[0] == 'name' and len(name_value) == 2 :
                        vds = name_value[1]
            elif vds != '' and keys[0] == '':
                volname = ''
                dev = ''
                for k in keys :
                    name_value = k.split('=')
                    if name_value[0] == 'vol' and len(name_value) == 2 :
                        volname = name_value[1]
                    elif name_value[0] == 'dev' and len(name_value) == 2 :
                        dev = name_value[1]
                        if re.match('^/dev/dsk/', dev) is None:
                            continue
                        dev = dev.replace('/dev/dsk/','/dev/rdsk/',1)
                        if re.match('^.*s[0-9]$',dev) is None:
                            dev = dev + 's2'
                        vdevname2dev[volname + '@' + vds ] = dev
            else:
                vds = ''

        cmd = ['/usr/sbin/ldm', 'list', '-o', 'disk', '-p',
               self.svc.resources_by_id[self.container_id].name]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            raise ex.excError
        for line in buff[0].split('\n'):
            keys = line.split('|')
            if keys[0] == 'VDISK' and len(keys) > 1 :
                for k in keys :
                    name_value = k.split('=')
                    if name_value[0] == 'vol' and len(name_value) == 2 :
                        vol = name_value[1]
                        if vol in vdevname2dev:
                            devs |= set([ vdevname2dev[vol] ])
        return devs
