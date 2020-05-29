from utilities.proc import justcall

ZONEADM="/usr/sbin/zoneadm"

def is_zone():
    out, err, ret = justcall(['zonename'])
    if ret != 0:
        return False
    if out.strip() == 'global':
        return False
    return True

class Zones(object):
    """Define zones (a.k.a. container) defined on a node"""
    def __init__(self):
        """scan node to get informations about its zones
        """
        self.zone_list = []
        self.zonename2zone = dict()
        self.zonepath2zone = dict()

        cmd = [ZONEADM, 'list', '-cip']
        (out, err, status) = justcall(cmd)
        if status == 0:
            #zoneid:zonename:state:zonepath:uuid:brand:ip-type
            for zoneadm_line in out.split('\n'):
                v = zoneadm_line.split(':')
                if len(v) != 7:
                    continue
                zone = Zone(zoneid=v[0], zonename=v[1], state=v[2],
                            zonepath=v[3], uuid=v[4], brand=v[5], ip_type=v[6])
                self.zonename2zone[zone.zonename] = zone
                self.zonepath2zone[zone.zonepath] = zone
                self.zone_list.append(zone)

    def refresh(self):
        """refresh zones information"""
        self.__init__()

    def zonename_from_zonepath(self, zonepath=None):
        """return zonename associated with zonepath, else return None"""
        if zonepath in self.zonepath2zone:
            return self.zonepath2zone[zonepath]
        else:
            return None

class Zone(object):
    def __init__(self, zoneid=None, zonename=None, state=None, zonepath=None,
                uuid=None, brand=None, ip_type=None ):
        """define Zone object attribute from zoneadm output line
                zoneid:zonename:state:zonepath:uuid:brand:ip-type
        """
        if state is None and zonename is not None:
            self.zonename = zonename
            self.refresh()
        else:
            self.zoneid = zoneid
            self.zonename = zonename
            self.state = state
            self.zonepath = zonepath
            self.uuid = uuid
            self.brand = brand
            self.ip_type = ip_type

    def refresh(self):
        """refresh zone information"""
        cmd = [ZONEADM, '-z', self.zonename, 'list', '-p']
        (out, err, status) = justcall(cmd)
        if status == 0:
            (self.zoneid, self.zonename, self.state, self.zonepath, self.uuid,
                self.brand, self.ip_type ) = out.split('\n')[0].split(':')
        else:
            print("fail to refresh zone informations for zonename", self.zonename)

if __name__ == "__main__":
    zones = Zones()
    print("Detected %s zones on system" % (len(zones.zone_list)))
    for zone in zones.zone_list:
        zonepath = zone.zonepath
        zonename = zone.zonename
        print("zonename=%s zonepath=%s zones.zonepath2zone[%s].zonename=%s" % (
            zonename, zonepath, zonepath, zones.zonepath2zone[zonepath].zonename
            ))
        z = Zone(zonename=zonename)
        print("zone %s : zoneid=%s, zonepath=%s brand=%s" % (zonename, z.zoneid,
                z.zonepath, z.brand))
