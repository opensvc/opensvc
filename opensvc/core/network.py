from __future__ import print_function
import json
import os
import re
import socket

import core.exceptions as ex
import foreign.six as six

from env import Env
from utilities.files import makedirs
from utilities.lazy import lazy
from utilities.net.ipaddress import ip_network, ip_address, summarize_address_range
from utilities.render.color import formatter


DRIVERS = {
    "routed_bridge": "RoutedBridge",
    "bridge": "Bridge",
    "loopback": "Loopback",
}

class NetworksMixin(object):

    ##########################################################################
    #
    # Exposed actions
    #
    ##########################################################################

    @formatter
    def network_ls(self):
        nets = self.networks_data()
        if self.options.format in ("json", "flat_json"):
            return nets
        print("\n".join([net for net in nets]))

    @formatter
    def network_show(self):
        data = {}
        for name, netdata in self.networks_data().items():
            if self.options.name and name != self.options.name:
                continue
            data[name] = netdata
        if self.options.format in ("json", "flat_json"):
            return data
        if not data:
            return
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        tree.load(data, title="networks")
        print(tree)

    def network_setup(self):
        data = self.networks_data()
        names = [name for name in data]
        if "default" not in names:
            names.append("default")
        for name in names:
            try:
                self.network_overlaps(name, data)
            except ex.Error as exc:
                self.log.warning("skip setup: %s", exc)
                continue
            self.network_create_config(name)
            self.network_create_bridge(name)
            self.network_create_routes(name)
            self.network_create_fwrules(name)

    @formatter
    def network_status(self):
        data = self.network_status_data(self.options.name)
        if self.options.format in ("json", "flat_json"):
            return data
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        head = tree.add_node()
        head.add_column("name", color.BOLD)
        head.add_column("type", color.BOLD)
        head.add_column("network", color.BOLD)
        head.add_column("size", color.BOLD)
        head.add_column("used", color.BOLD)
        head.add_column("free", color.BOLD)
        head.add_column("pct", color.BOLD)
        for name in sorted(data):
            ndata = data[name]
            net_node = head.add_node()
            net_node.add_column(name, color.BROWN)
            net_node.add_column(data[name]["type"])
            net_node.add_column(data[name]["network"])
            net_node.add_column("%d" % data[name]["size"])
            net_node.add_column("%d" % data[name]["used"])
            net_node.add_column("%d" % data[name]["free"])
            net_node.add_column("%.2f%%" % data[name]["pct"])
            if not self.options.verbose:
                continue
            ips_node = net_node.add_node()
            ips_node.add_column("ip", color.BOLD)
            ips_node.add_column("node", color.BOLD)
            ips_node.add_column("service", color.BOLD)
            ips_node.add_column("resource", color.BOLD)
            for ip in sorted(ndata.get("ips", []), key=lambda x: (x["ip"], x["node"], x["path"], x["rid"])):
                ip_node = ips_node.add_node()
                ip_node.add_column(ip["ip"])
                ip_node.add_column(ip["node"])
                ip_node.add_column(ip["path"])
                ip_node.add_column(ip["rid"])
        print(tree)

    ##########################################################################
    #
    # Internal methods
    #
    ##########################################################################

    @lazy
    def cni_config(self):
        try:
            return self.conf_get("cni", "config").rstrip("/")
        except ex.OptNotFound as exc:
            return exc.default

    def network_data(self, id, nets=None):
        if nets is None:
            nets = self.networks_data()
        if id:
            return nets[id]
        else:
            return nets

    def networks_data_from_cni_confs(self):
        import glob
        nets = {}
        for cf in glob.glob(self.cni_config+"/*.conf"):
            try:
                with open(cf, "r") as ofile:
                    data = json.load(ofile)
            except ValueError:
                data = {}
            if data.get("type") == "portmap":
                continue
            name = os.path.basename(cf)
            name = re.sub(".conf$", "", name)
            nets[name] = {
                "cni": {
                    "cf": cf,
                    "mtime": os.path.getmtime(cf),
                    "data": data,
                },
                "config": {
                    "type": "undef",
                    "network": "undef",
                },
            }
        return nets

    def networks_data(self):
        nets = self.networks_data_from_cni_confs()
        sections = list(self.conf_sections("network"))
        if "network#default" not in sections:
            sections.append("network#default")
        for section in sections:
            _, name = section.split("#", 1)
            config = {}
            config["type"] = self.oget(section, "type")
            for key in self.section_kwargs(section, config["type"]):
                config[key] = self.oget(section, key, rtype=config["type"])
                if config["type"] == "routed_bridge":
                    config["subnets"] = self.oget_scopes(section, "subnet", rtype=config["type"])
            if not config:
                continue
            routes = self.routes(name, config)
            if config["type"] == "routed_bridge" and not any(config["subnets"][n] for n in config["subnets"]):
                self.log.info("initial %s routed_bridge network subnets assignment:", name)
                kws = []
                for route in routes:
                    kw = "network#%s.subnet@%s=%s" % (name, route["node"], route["dst"])
                    kws.append(kw)
                    self.log.info(" %s", kw)
                from core.objects.ccfg import Ccfg
                svc = Ccfg()
                svc.set_multi(kws, validation=False)
                self.unset_lazy("cd")
                config["subnets"] = self.oget_scopes(section, "subnet", rtype="routed_bridge")
            if name not in nets:
                nets[name] = {}
            nets[name]["config"] = config
            nets[name]["routes"] = routes
            nets[name]["tables"] = self.tables(name)
        nets["lo"] = {
            "config": {
                "type": "loopback",
                "network": "127.0.0.1/32",
                "tables": ["main"],
            },
        }
        return nets

    def node_subnet(self, name, nodename=None, config=None):
        if nodename is None:
            nodename = Env.nodename
        if not config:
            config = self.network_data(name)["config"]
        persistent_subnet = config.get("subnets", {}).get(nodename)
        if persistent_subnet:
            return ip_network(six.text_type(persistent_subnet))
        idx = self.cluster_nodes.index(nodename)
        network = config["network"]
        ips_per_node = config["ips_per_node"]
        ips_per_node = 1 << (ips_per_node - 1).bit_length()
        net = ip_network(six.text_type(network))
        first = net[0] + (idx * ips_per_node)
        last = first + ips_per_node - 1
        subnet = next(summarize_address_range(first, last))
        return subnet

    def tables(self, name):
        try:
            return self.oget("network#"+name, "tables")
        except:
            return

    def find_node_ip(self, nodename, af=socket.AF_INET):
        try:
            data = socket.getaddrinfo(nodename, None)
        except socket.gaierror:
            raise ex.Error("node %s is not resolvable" % nodename)
        for d in data:
            _af, _, _, _, addr = d
            if _af != af:
                continue
            addr = addr[0]
            if addr in ("127.0.0.1", "127.0.1.1", "::1") or addr.startswith("fe80:"):
                continue
            return addr
        raise ex.Error("node %s has no %s address" % (nodename, af))

    def routes(self, name, config=None):
        routes = []
        if not config:
            config = self.network_data(name)["config"]
        ntype = config["type"]
        if ntype != "routed_bridge":
            return routes
        network = config.get("network")
        if not network:
            return []
        if ":" in network:
            af = socket.AF_INET6
        else:
            af = socket.AF_INET
        try:
            local_ip = self.oget("network#"+name, "addr")
        except ValueError:
            local_ip = None
        if local_ip is None:
            try:
                local_ip = self.find_node_ip(Env.nodename, af=af)
            except ex.Error as exc:
                self.log.warning("%s", exc)
                return routes
        for nodename in self.cluster_nodes:
            for table in config["tables"]:
                if nodename == Env.nodename:
                    routes.append({
                        "node": nodename,
                        "dst": str(self.node_subnet(name, nodename, config=config)),
                        "dev": "obr_"+name,
                        "brdev": "obr_"+name,
                        "table": table,
                    })
                    continue
                try:
                    gw = self.find_node_ip(nodename, af=af)
                except ex.Error as exc:
                    self.log.warning("%s", exc)
                    continue
                routes.append({
                    "local_ip": local_ip,
                    "node": nodename,
                    "dst": str(self.node_subnet(name, nodename, config=config)),
                    "gw": gw,
                    "brdev": "obr_"+name,
                    "brip": self.network_bridge_ip(name, config=config),
                    "table": table,
                    "tunnel": config["tunnel"],
                })
        return routes

    def network_overlaps(self, name, nets=None):
        def get_val(key, net):
            try:
                return net["config"][key]
            except KeyError:
                try:
                    return net["cni"]["data"][key]
                except KeyError:
                    return
        if nets is None:
            nets = self.networks_data()
        net = nets.get(name)
        if not net:
            return
        try:
            network = ip_network(six.text_type(get_val("network", net)))
        except Exception:
            return
        for other_name, other in nets.items():
            if name == other_name:
                continue
            try:
                other_network = ip_network(six.text_type(get_val("network", other)))
            except Exception:
                continue
            if other_network and network.overlaps(other_network):
                raise ex.Error("network %s %s overlaps with %s %s" % \
                                  (name, network, other_name, other_network))

    def network_create_fwrules(self, name):
        """
        OS specific
        """
        pass

    def network_bridge_ip(self, name, config=None):
        net = self.node_subnet(name, config=config)
        ip = str(net[1])+"/"+str(net.prefixlen)
        return ip

    def network_create_bridge(self, name, nets=None):
        data = self.network_data(name, nets=nets)
        ntype = data["config"]["type"]
        if ntype != "routed_bridge":
            return
        ip = self.network_bridge_ip(name, config=data["config"])
        self.network_bridge_add("obr_"+name, ip)

    def network_bridge_add(self, *args, **kwargs):
        """
        OS specific
        """
        pass

    def network_create_routes(self, name):
        routes = self.routes(name)
        for route in routes:
            self.network_route_add(**route)

    def network_route_add(self, *args, **kwargs):
        """
        OS specific
        """
        pass

    def network_create_config(self, name="default", nets=None):
        try:
            data = self.network_data(name, nets=nets)
        except KeyError:
            raise ex.Error("network %s does not exist" % name)
        ntype = data["config"]["type"]
        fn = "network_create_%s_config" % ntype
        if hasattr(self, fn):
            getattr(self, fn)(name, nets=nets)

    def network_create_weave_config(self, name="default", nets=None):
        cf = os.path.join(self.cni_config, name+".conf")
        if os.path.exists(cf):
            return
        self.log.info("create %s", cf)
        data = self.network_data(name, nets=nets)
        network = data["config"]["network"]
        conf = {
            "cniVersion": "0.3.0",
            "name": name,
            "type": "weave-net",
            "ipam": {
                "subnet": network,
            },
        }
        makedirs(self.cni_config)
        with open(cf, "w") as ofile:
            json.dump(conf, ofile, indent=4)

    def network_create_routed_bridge_config(self, name="default", nets=None):
        config = self.network_data(name, nets=nets)["config"]
        subnet = str(self.node_subnet(name, config=config))
        network = config["network"]
        brip = self.network_bridge_ip(name, config=config).split("/")[0]
        cf = os.path.join(self.cni_config, name+".conf")
        if os.path.exists(cf):
            return
        if ":" in network:
            default = "::/0"
        else:
            default = "0.0.0.0/0"
        self.log.info("create %s", cf)
        conf = {
            "cniVersion": "0.3.0",
            "name": name,
            "type": "bridge",
            "bridge": "obr_"+name,
            "isGateway": True,
            "ipMasq": False,
            "ipam": {
                "type": "host-local",
                "subnet": subnet,
                "routes": [
                    { "dst": default },
                    { "dst": network, "gw": brip },
                ]
            }
        }
        makedirs(self.cni_config)
        with open(cf, "w") as ofile:
            json.dump(conf, ofile, indent=4)

    def network_create_bridge_config(self, name="default", nets=None):
        cf = os.path.join(self.cni_config, name+".conf")
        if os.path.exists(cf):
            return
        self.log.info("create %s", cf)
        conf = {
            "cniVersion": "0.3.0",
            "name": name,
            "type": "bridge",
            "bridge": "obr_"+name,
            "isGateway": True,
            "ipMasq": True,
            "ipam": {
                "type": "host-local",
                "routes": [
                    { "dst": "0.0.0.0/0" }
                ]
            }
        }
        makedirs(self.cni_config)
        data = self.network_data(name, nets=nets)
        network = data["config"]["network"]
        conf["ipam"]["subnet"] = network
        with open(cf, "w") as ofile:
            json.dump(conf, ofile, indent=4)

    def network_create_loopback_config(self, name="lo", nets=None):
        cf = os.path.join(self.cni_config, name+".conf")
        if os.path.exists(cf):
            return
        self.log.info("create %s", cf)
        conf = {
            "cniVersion": "0.3.0",
            "name": name,
            "type": "loopback",
        }
        makedirs(self.cni_config)
        data = self.network_data(name, nets=nets)
        try:
            network = data["config"]["network"]
            conf["ipam"]["subnet"] = network
        except KeyError:
            pass
        with open(cf, "w") as ofile:
            json.dump(conf, ofile, indent=4)

    def network_ip_data(self):
        data = []
        try:
            cdata = self._daemon_status(silent=True).get("monitor", {}).get("nodes", {})
        except Exception:
            cdata = {}
        for nodename, node in cdata.items():
            for path, sdata in node.get("services", {}).get("status", {}).items():
                for rid, rdata in sdata.get("resources", {}).items():
                    ip = rdata.get("info", {}).get("ipaddr")
                    if not ip:
                        continue
                    data.append({
                        "ip": ip,
                        "node": nodename,
                        "path": path,
                        "rid": rid,
                    })
        return data

    def network_status_data(self, name=None):
        data = {}
        nets = self.networks_data()
        ipdata = self.network_ip_data()
        for _name, ndata in nets.items():
            if name and name != _name:
                continue
            try:
                network = ip_network(six.text_type(ndata["config"]["network"]))
                _data = {
                    "type": ndata["config"]["type"],
                    "network": ndata["config"]["network"],
                    "size": network.num_addresses,
                    "ips": [],
                }
            except Exception:
                network = None
                _data = {
                    "type": ndata["config"]["type"],
                    "network": ndata["config"]["network"],
                    "size": 1,
                    "ips": [],
                }
            for idata in ipdata:
                ip = ip_address(idata["ip"])
                if not network or ip not in network:
                    continue
                _data["ips"].append(idata)
            _data["used"] = len(_data["ips"])
            _data["free"] = _data["size"] - _data["used"]
            _data["pct"] = 100 * _data["used"] / _data["size"]
            data[_name] = _data
        return data


