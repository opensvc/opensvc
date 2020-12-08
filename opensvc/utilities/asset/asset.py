import codecs
import datetime
import os

import core.exceptions as ex
from utilities.converters import print_size
from env import Env
from utilities.proc import justcall, which

class BaseAsset(object):
    s_config = "config"
    s_probe = "probe"
    s_default = "default"

    def __init__(self, node):
        self.node = node

    def get_mem_bytes(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'mem_bytes')
            s = str(s/1024/1024)
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            try:
                s = self._get_mem_bytes()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "mem",
            "value": s,
            "source": source,
            "formatted_value": print_size(s)
        }

    def get_mem_banks(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'mem_banks')
            s = str(s)
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            try:
                s = self._get_mem_banks()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "mem banks",
            "value": s,
            "source": source
        }

    def get_mem_slots(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'mem_slots')
            s = str(s)
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            try:
                s = self._get_mem_slots()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "mem slots",
            "value": s,
            "source": source
        }

    def get_os_vendor(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'os_vendor')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_os_vendor()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "os vendor",
            "value": s,
            "source": source
        }

    def get_os_release(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'os_release')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_os_release()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "os release",
            "value": s,
            "source": source
        }

    def get_os_kernel(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'os_kernel')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_os_kernel()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "os kernel",
            "value": s,
            "source": source
        }

    def get_os_arch(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'os_arch')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_os_arch()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "os arch",
            "value": s,
            "source": source
        }

    def get_cpu_freq(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'cpu_freq')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_cpu_freq()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "cpu freq",
            "value": s,
            "source": source
        }

    def get_cpu_threads(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'cpu_threads')
            s = str(s)
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            try:
                s = self._get_cpu_threads()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "cpu threads",
            "value": s,
            "source": source
        }

    def get_cpu_cores(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'cpu_cores')
            s = str(s)
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            try:
                s = self._get_cpu_cores()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "cpu cores",
            "value": s,
            "source": source
        }

    def get_cpu_dies(self):
        s = '0'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'cpu_dies')
            s = str(s)
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            try:
                s = self._get_cpu_dies()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "cpu dies",
            "value": s,
            "source": source
        }

    def get_cpu_model(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'cpu_model')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_cpu_model()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "cpu model",
            "value": s,
            "source": source
        }

    def get_serial(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'serial')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_serial()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "serial",
            "value": s,
            "source": source
        }

    def get_bios_version(self):
        s = ''
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'bios_version')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_bios_version()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "bios version",
            "value": s,
            "source": source
        }

    def get_sp_version(self):
        s = ''
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'sp_version')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_sp_version()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "sp version",
            "value": s,
            "source": source
        }

    def get_enclosure(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'enclosure')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_enclosure()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "enclosure",
            "value": s,
            "source": source
        }

    def get_tz(self):
        s = None
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'tz')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_tz()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "timezone",
            "value": s,
            "source": source
        }

    def _get_tz(self):
        cmd = ["date", "+%z"]
        out, err, ret = justcall(cmd)
        out = out.strip()
        if len(out) != 5:
            return
        return out[:3] + ":" + out[3:]

    def get_connect_to(self):
        s = None
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'connect_to')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_connect_to()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "listener address",
            "value": s,
            "source": source
        }

    def _get_connect_to(self):
        if self.data["model"]["value"] != "Google":
            return
        if not which("gcloud"):
            return
        cmd = ["gcloud", "compute", "instances", "describe", "-q", "--format", "json", Env.nodename]
        out, err, ret = justcall(cmd)
        return self._parse_connect_to(out)

    def _parse_connect_to(self, out):
        """
	  "networkInterfaces": [
	    {
	      "accessConfigs": [
		{
		  "kind": "compute#accessConfig",
		  "name": "external-nat",
		  "natIP": "23.251.137.71",
		  "type": "ONE_TO_ONE_NAT"
		}
	      ],
	      "name": "nic0",
	      "networkIP": "10.132.0.2",
	    }
        """
        import json
        try:
            data = json.loads(out)
        except:
            return
        nics = [d for d in data["networkInterfaces"] if len(d["accessConfigs"]) > 0]
        if len(nics) == 0:
            return
        for nic in nics:
            if nic["name"] == "nic0":
                return nic["accessConfigs"][0]["natIP"]
        return nics[0]["accessConfigs"][0]["natIP"]

    def get_manufacturer(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'manufacturer')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_manufacturer()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "manufacturer",
            "value": s,
            "source": source
        }

    def get_model(self):
        s = 'Unknown'
        source = self.s_default
        try:
            s = self.node.conf_get('node', 'model')
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound):
            try:
                s = self._get_model()
                source = self.s_probe
            except AttributeError:
                pass
        return {
            "title": "model",
            "value": s,
            "source": source
        }

    def get_version(self):
        s = self.node.agent_version
        return {
            "title": "agent version",
            "value": s,
            "source": self.s_probe
        }

    def get_cluster_id(self):
        s = self.node.cluster_id
        return {
            "title": "cluster id",
            "value": s,
            "source": self.s_probe
        }

    def get_listener_port(self):
        s = str(Env.listener_port)
        source = self.s_default
        try:
            s = str(self.node.conf_get('listener', 'port'))
            source = self.s_config
        except (ex.OptNotFound, ex.RequiredOptNotFound, ValueError, TypeError):
            pass
        return {
            "title": "listener port",
            "value": s,
            "source": source
        }

    def get_from_conf(self, section, kw, title):
        try:
            s = self.node.conf_get(section, kw)
            source = self.s_config
        except ex.OptNotFound as exc:
            if exc.default is None:
                return None
            s = exc.default
            source = self.s_default
        return {
            "title": title,
            "value": s,
            "source": source
        }

    def get_node_env(self):
        return self.get_from_conf("node", "env", "environment")

    def get_sec_zone(self):
        return self.get_from_conf("node", "sec_zone", "security zone")

    def get_asset_env(self):
        return self.get_from_conf("node", "asset_env", "asset environment")

    def get_loc_country(self):
        return self.get_from_conf("node", "loc_country", "loc, country")

    def get_loc_city(self):
        return self.get_from_conf("node", "loc_city", "loc, city")

    def get_loc_addr(self):
        return self.get_from_conf("node", "loc_addr", "loc, addr")

    def get_loc_building(self):
        return self.get_from_conf("node", "loc_building", "loc, building")

    def get_loc_floor(self):
        return self.get_from_conf("node", "loc_floor", "loc, floor")

    def get_loc_room(self):
        return self.get_from_conf("node", "loc_room", "loc, room")

    def get_loc_rack(self):
        return self.get_from_conf("node", "loc_rack", "loc, rack")

    def get_loc_zip(self):
        return self.get_from_conf("node", "loc_zip", "loc, zip")

    def get_team_integ(self):
        return self.get_from_conf("node", "team_integ", "team, integ")

    def get_team_support(self):
        return self.get_from_conf("node", "team_support", "team, support")

    def get_hba(self):
        try:
            hba = self._get_hba()
        except AttributeError:
            hba = []
        return hba

    def get_targets(self):
        try:
            s = self._get_targets()
        except AttributeError:
            s = []
        return s

    def get_hardware(self):
        try:
            s = self._get_hardware()
        except AttributeError:
            s = []
        return s

    def get_uids(self):
        return self.get_ids("/etc/passwd", ("username", "uid"))

    def get_gids(self):
        return self.get_ids("/etc/group", ("groupname", "gid"))

    def get_ids(self, p, keys):
        if Env.sysname == "Windows":
            return []
        if not os.path.exists(p):
            return []
        try:
            with codecs.open(p, "r", "utf8") as f:
                buff = f.read()
        except:
            with codecs.open(p, "r", "latin1") as f:
                buff = f.read()
        d = []
        for line in buff.split('\n'):
            line = line.strip()
            if line.startswith("#"):
                continue
            l = line.split(':')
            if len(l) < 3:
                continue
            try:
                i = int(l[2])
            except:
                continue
            name = l[0]
            d.append({
                keys[0]: name,
                keys[1]: l[2]
            })
        return d

    def get_lan(self):
        kwargs = {'mcast': True}
        if Env.sysname == 'HP-UX':
            kwargs['hwaddr'] = True
        import utilities.ifconfig
        ifconfig = utilities.ifconfig.Ifconfig(**kwargs)
        lan = {}
        for intf in ifconfig.intf:
            if len(intf.hwaddr) == 0:
                continue
            if intf.hwaddr not in lan:
                lan[intf.hwaddr] = []
            if type(intf.ipaddr) == str and intf.ipaddr != '':
                d = {'type': 'ipv4',
                     'intf': intf.name,
                     'addr': intf.ipaddr,
                     'mask': intf.mask,
                     'flag_deprecated': intf.flag_deprecated,
                    }
                lan[intf.hwaddr] += [d]
            elif type(intf.ipaddr) == list:
                for i, ip in enumerate(intf.ipaddr):
                    if ip != '':
                        d = {'type': 'ipv4',
                             'intf': intf.name,
                             'addr': ip,
                             'mask': intf.mask[i],
                             'flag_deprecated': intf.flag_deprecated,
                            }
                    lan[intf.hwaddr] += [d]
            for i, ip6 in enumerate(intf.ip6addr):
                d = {'type': 'ipv6',
                     'intf': intf.name,
                     'addr': intf.ip6addr[i],
                     'mask': intf.ip6mask[i],
                     'flag_deprecated': intf.flag_deprecated,
                    }
                lan[intf.hwaddr] += [d]
            if intf.name in ifconfig.mcast_data:
                for addr in ifconfig.mcast_data[intf.name]:
                    if ':' in addr:
                        addr_type = 'ipv6'
                    else:
                        addr_type = 'ipv4'
                    d = {'type': addr_type,
                         'intf': intf.name,
                         'addr': addr,
                         'mask': "",
                         'flag_deprecated': intf.flag_deprecated,
                        }
                    lan[intf.hwaddr] += [d]

        return lan

    def get_boot_id(self):
        return str(os.path.getmtime("/proc/1"))

    def get_last_boot(self):
        cmd = ["/usr/bin/uptime"]
        out, err, ret = justcall(cmd)
        l = out.split()

        i = 0
        for s in ("days,", "day(s),", "day,", "days", "day"):
            try:
                i = l.index(s)
                break
            except:
                pass

        if i == 0:
            last = datetime.datetime.now()
        else:
            try:
                last = datetime.datetime.now() - datetime.timedelta(days=int(l[i-1]))
            except:
                return

        last = last.strftime("%Y-%m-%d")
        return {
            "title": "last boot",
            "value": last,
            "source": self.s_probe
        }

    def get_asset_dict(self):
        self.data = {}
        self.data['nodename'] = {
            "title": "nodename",
            "value": Env.nodename,
            "source": self.s_probe
        }
        self.data['fqdn'] = {
            "title": "fqdn",
            "value": Env.fqdn,
            "source": self.s_probe
        }
        self.data['version'] = self.get_version()
        self.data['os_name'] = {
            "title": "os name",
            "value": Env.sysname,
            "source": self.s_probe
        }
        self.data['os_vendor'] = self.get_os_vendor()
        self.data['os_release'] = self.get_os_release()
        self.data['os_kernel'] = self.get_os_kernel()
        self.data['os_arch'] = self.get_os_arch()
        self.data['mem_bytes'] = self.get_mem_bytes()
        self.data['mem_banks'] = self.get_mem_banks()
        self.data['mem_slots'] = self.get_mem_slots()
        self.data['cpu_freq'] = self.get_cpu_freq()
        self.data['cpu_threads'] = self.get_cpu_threads()
        self.data['cpu_cores'] = self.get_cpu_cores()
        self.data['cpu_dies'] = self.get_cpu_dies()
        self.data['cpu_model'] = self.get_cpu_model()
        self.data['serial'] = self.get_serial()
        self.data['model'] = self.get_model()
        self.data['manufacturer'] = self.get_manufacturer()
        self.data['bios_version'] = self.get_bios_version()
        self.data['sp_version'] = self.get_sp_version()
        self.data['node_env'] = self.get_node_env()
        self.data['enclosure'] = self.get_enclosure()
        self.data['listener_port'] = self.get_listener_port()
        self.data['cluster_id'] = self.get_cluster_id()
        connect_to = self.get_connect_to()
        if connect_to is not None:
            self.data['connect_to'] = connect_to
        last_boot = self.get_last_boot()
        if last_boot is not None:
            self.data['last_boot'] = last_boot
        sec_zone = self.get_sec_zone()
        if sec_zone is not None:
            self.data['sec_zone'] = sec_zone
        asset_env = self.get_asset_env()
        if asset_env is not None:
            self.data['asset_env'] = asset_env
        tz = self.get_tz()
        if tz is not None:
            self.data['tz'] = tz
        loc_country = self.get_loc_country()
        if loc_country is not None:
            self.data['loc_country'] = loc_country
        loc_city = self.get_loc_city()
        if loc_city is not None:
            self.data['loc_city'] = loc_city
        loc_building = self.get_loc_building()
        if loc_building is not None:
            self.data['loc_building'] = loc_building
        loc_room = self.get_loc_room()
        if loc_room is not None:
            self.data['loc_room'] = loc_room
        loc_rack = self.get_loc_rack()
        if loc_rack is not None:
            self.data['loc_rack'] = loc_rack
        loc_addr = self.get_loc_addr()
        if loc_addr is not None:
            self.data['loc_addr'] = loc_addr
        loc_floor = self.get_loc_floor()
        if loc_floor is not None:
            self.data['loc_floor'] = loc_floor
        loc_zip = self.get_loc_zip()
        if loc_zip is not None:
            self.data['loc_zip'] = loc_zip
        team_integ = self.get_team_integ()
        if team_integ is not None:
            self.data['team_integ'] = team_integ
        team_support = self.get_team_support()
        if team_support is not None:
            self.data['team_support'] = team_support
        hardware = self.get_hardware()
        if hardware is not None:
            self.data['hardware'] = hardware
        hba = self.get_hba()
        if hba is not None:
            self.data['hba'] = hba
        targets = self.get_targets()
        if targets is not None:
            self.data['targets'] = targets
        lan = self.get_lan()
        if lan is not None:
            self.data['lan'] = lan
        uids = self.get_uids()
        if uids is not None:
            self.data['uids'] = uids
        gids = self.get_gids()
        if gids is not None:
            self.data['gids'] = gids
        return self.data
