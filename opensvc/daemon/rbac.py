import daemon.shared as shared
from utilities.naming import split_path, factory

class ObjectCreateMixin(object):
    def rbac_create_data(self, payload=None , thr=None, **kwargs):
        if thr.usr is False:
            return
        if not payload:
            return
        all_ns = thr.get_all_ns()
        grants = thr.user_grants(all_ns)
        if "root" in grants:
            return []
        errors = []
        for path, cd in payload.items():
            errors += self.rbac_create_obj(path, cd, all_ns, thr=thr, **kwargs)
        return errors

    def rbac_create_obj(self, path, cd, all_ns, thr=None, **kwargs):
        errors = []
        name, namespace, kind = split_path(path)
        if namespace is None:
            namespace = "root"
        grants = thr.user_grants(all_ns | set([namespace]))
        if kind == "nscfg":
            if "squatter" not in grants:
                errors.append("%s: create the namespace %s config requires the squatter cluster role" % (path, namespace))
                return errors
            elif namespace not in grants["admin"]:
                thr.usr.set_multi(["grant+=admin:%s" % namespace])
                grants["admin"].add(namespace)
        elif namespace not in all_ns:
            if namespace == "system":
                errors.append("%s: create the new namespace system requires the root cluster role")
                return errors
            elif "squatter" not in grants:
                errors.append("%s: create the new namespace %s requires the squatter cluster role" % (path, namespace))
                return errors
            elif namespace not in grants["admin"]:
                thr.usr.set_multi(["grant+=admin:%s" % namespace])
                grants["admin"].add(namespace)
        if not "root" in grants and not "prioritizer" in grants:
            try:
                del cd["DEFAULT"]["priority"]
            except KeyError:
                pass
        thr.rbac_requires(roles=["admin"], namespaces=[namespace], grants=grants, **kwargs)
        try:
            orig_obj = factory(kind)(name, namespace=namespace, volatile=True, node=shared.NODE)
        except:
            orig_obj = None
        try:
            obj = factory(kind)(name, namespace=namespace, volatile=True, cd=cd, node=shared.NODE)
        except Exception as exc:
            errors.append("%s: unbuildable config: %s" % (path, exc))
            return errors
        if kind == "vol":
            errors.append("%s: volume create requires the root privilege" % path)
        elif kind == "ccfg":
            errors.append("%s: cluster config create requires the root privilege" % path)
        elif kind == "svc":
            groups = ["disk", "fs", "app", "share", "sync"]
            for r in obj.get_resources(groups):
                if r.rid == "sync#i0":
                    continue
                errors.append("%s: resource %s requires the root privilege" % (path, r.rid))
            for r in obj.get_resources("task"):
                if r.type not in ("task.podman", "task.docker"):
                    errors.append("%s: resource %s type %s requires the root privilege" % (path, r.rid, r.type))
            for r in obj.get_resources("container"):
                if r.type not in ("container.podman", "container.docker"):
                    errors.append("%s: resource %s type %s requires the root privilege" % (path, r.rid, r.type))
            for r in obj.get_resources("ip"):
                if r.type not in ("ip.cni"):
                    errors.append("%s: resource %s type %s requires the root privilege" % (path, r.rid, r.type))
        for section, sdata in cd.items():
            rtype = cd[section].get("type")
            errors += self.rbac_create_data_section(path, section, rtype, sdata, grants, obj, orig_obj, all_ns, thr=thr)
        return errors

    def rbac_create_data_section(self, path, section, rtype, sdata, user_grants, obj, orig_obj, all_ns, thr=None):
        errors = []
        for key, val in sdata.items():
            if "trigger" in key or key.startswith("pre_") or key.startswith("post_") or key.startswith("blocking_"):
                errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, val))
                continue
            _key = key.split("@")[0]
            try:
                _val = obj.oget(section, _key)
            except Exception as exc:
                errors.append("%s: %s" % (path, exc))
                continue
            # scopable
            for n in obj.nodes | obj.drpnodes:
                _val = obj.oget(section, _key, impersonate=n)
                if _key in ("container_data_dir") and _val:
                    if _val.startswith("/"):
                        errors.append("%s: keyword %s.%s=%s host paths require the root role" % (path, section, key, _val))
                        continue
                if _key in ("devices", "volume_mounts") and _val:
                    _errors = []
                    for __val in _val:
                        if __val.startswith("/"):
                            _errors.append("%s: keyword %s.%s=%s host paths require the root role" % (path, section, key, __val))
                            continue
                    if _errors:
                        errors += _errors
                        break
                if section == "DEFAULT" and _key == "monitor_action" and _val not in ("freezestop", "switch", None):
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
                if section.startswith("container#") and _key == "netns" and _val == "host":
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
                if section.startswith("container#") and _key == "privileged" and _val not in ("false", False, None):
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
                if section.startswith("ip#") and _key == "netns" and _val in (None, "host"):
                    errors.append("%s: keyword %s.%s=%s requires the root role" % (path, section, key, _val))
                    break
            # unscopable
            if section == "DEFAULT" and _key == "cn":
                errors += self.rbac_kw_cn(path, _val, orig_obj)
            elif section == "DEFAULT" and _key == "grant":
                errors += self.rbac_kw_grant(path, _val, user_grants, all_ns, thr=thr)
        return errors

    def rbac_kw_grant(self, path, val, user_grants, all_ns, thr=None):
        errors = []
        req_grants = thr.parse_grants(val, all_ns)
        for role, namespaces in req_grants.items():
            if namespaces is None:
                # cluster roles
                if role not in user_grants:
                    errors.append("%s: keyword grant=%s requires the %s cluster role" % (path, val, role))
            else:
                # namespaces roles
                delta = set(namespaces) - set(user_grants.get(role, []))
                if delta:
                    delta = sorted(list(delta))
                    errors.append("%s: keyword grant=%s requires the %s:%s privilege" % (path, val, role, ",".join(delta)))
        return errors

    def rbac_kw_cn(self, path, val, orig_obj):
        errors = []
        try:
            orig_cn = orig_obj.oget("DEFAULT", "cn")
        except Exception:
            orig_cn = None
        if orig_cn == val:
            return []
        errors.append("%s: keyword cn=%s requires the root role" % (path, val))
        return errors

