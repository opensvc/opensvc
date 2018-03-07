from __future__ import print_function

import sys
import re
import os

import rcExceptions as ex
from converters import *
from rcUtilities import is_string, try_decode, read_cf, eval_expr, unset_lazy, \
                        lazy, unset_all_lazy
from rcGlobalEnv import rcEnv

class ExtConfig(object):
    @lazy
    def has_default_section(self):
        if hasattr(self, "svcname"):
            return True
        else:
            return False

    def unset(self):
        """
        The 'unset' action entrypoint.
        Verifies the --param and --value are set, and finally call the _unset
        internal method.
        """
        if self.options.kw:
            kw = self.options.kw
        elif self.options.param:
            kw = [self.options.param]
        else:
            kw = None
        if kw is None:
            print("no keyword specified. set --kw <keyword>", file=sys.stderr)
            return 1
        ret = 0
        for _kw in kw:
            ret += self.unset_one(_kw)
        return ret

    def unset_one(self, kw):
        elements = kw.split('.')
        if self.has_default_section and len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            print("malformed parameter. format as 'section.key'",
                  file=sys.stderr)
            return 1
        section, option = elements
        if section in self.default_status_groups:
            err = 0
            for rid in [rid for rid in self.config.sections() if rid.startswith(section+"#")]:
                try:
                    self._unset(rid, option)
                except ex.excError as exc:
                    print(exc, file=sys.stderr)
                    err += 1
            return err
        else:
            try:
                self._unset(section, option)
                return 0
            except ex.excError as exc:
                print(exc, file=sys.stderr)
                return 1

    def _unset(self, section, option):
        """
        Delete an option in the service configuration file specified section.
        """
        lines = self._read_cf().splitlines()
        lines = self.unset_line(lines, section, option)
        try:
            self._write_cf(lines)
        except (IOError, OSError) as exc:
            raise ex.excError(str(exc))
        unset_all_lazy(self)
        if hasattr(self, "ref_cache"):
            delattr(self, "ref_cache")

    def unset_line(self, lines, section, option):
        section = "[%s]" % section
        need_write = False
        in_section = False
        for i, line in enumerate(lines):
            sline = line.strip()
            if sline == section:
                in_section = True
            elif in_section:
                if sline.startswith("["):
                    break
                elif "=" in sline:
                    elements = sline.split("=")
                    _option = elements[0].strip()
                    if option != _option:
                        continue
                    del lines[i]
                    need_write = True
                    while i < len(lines) and "=" not in lines[i] and \
                          not lines[i].strip().startswith("[") and \
                          lines[i].strip() != "":
                        del lines[i]

        if not in_section:
            raise ex.excError("section %s not found" % section)

        if not need_write:
            raise ex.excError("option '%s' not found in section %s" % (option, section))

        return lines

    def get(self):
        """
        The 'get' action entrypoint.
        Verifies the --param or --kw is set, set DEFAULT as section if no section was
        specified, and finally print,
        * the raw value if --eval is not set
        * the dereferenced and evaluated value if --eval is set
        """
        if self.options.kw and len(self.options.kw) == 1:
            kw = self.options.kw[0]
        elif self.options.param:
            kw = self.options.param
        else:
            kw = None
        try:
            print(self._get(kw, self.options.eval))
        except ex.OptNotFound as exc:
            print(exc.default)
        except ex.RequiredOptNotFound as exc:
            return 1
        except ex.excError as exc:
            print(exc, file=sys.stderr)
            return 1
        except Exception:
            return 1
        return 0

    def _get(self, param=None, evaluate=False):
        """
        Verifies the param is set, set DEFAULT as section if no section was
        specified, and finally return,
        * the raw value if evaluate is False
        * the dereferenced and evaluated value if evaluate is True
        """
        if param is None:
            raise ex.excError("no parameter. set --param")
        elements = param.split('.')
        if self.has_default_section and len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            raise ex.excError("malformed parameter. format as 'section.key'")
        section, option = elements
        if section == "DEFAULT" and not self.has_default_section:
            raise ex.excError("the DEFAULT section is not allowed in %s" % self.paths.cf)
        if not self.config.has_section(section):
            if section != 'DEFAULT' and self.has_default_section:
                raise ex.excError("section [%s] not found" % section)
            if not self.has_default_section:
                raise ex.excError("section [%s] not found" % section)
        if evaluate:
            return self.conf_get(section, option, "string", scope=True)
        else:
            return self.config.get(section, option)

    def set(self):
        """
        The 'set' action entrypoint.
        Verifies the --param and --value are set, set DEFAULT as section
        if no section was specified, and set the value using the internal
        _set() method.
        """
        if self.options.kw is not None:
            return self.set_multi(self.options.kw)
        else:
            return self.set_mono()

    def set_multi(self, kws):
        changes = []
        self.set_multi_cache = {}
        for kw in kws:
            if "=" not in kw:
                raise ex.excError("malformed kw expression: %s: no '='" % kw)
            keyword, value = kw.split("=", 1)
            if keyword[-1] == "-":
                op = "remove"
                keyword = keyword[:-1]
            elif keyword[-1] == "+":
                op = "add"
                keyword = keyword[:-1]
            else:
                op = "set"
            index = None
            if "[" in keyword:
                keyword, right = keyword.split("[", 1)
                if not right.endswith("]"):
                    raise ex.excError("malformed kw expression: %s: no trailing"
                                      " ']' at the end of keyword" % kw)
                try:
                    index = int(right[:-1])
                except ValueError:
                    raise ex.excError("malformed kw expression: %s: index is "
                                      "not integer" % kw)
            if "." in keyword and "#" not in keyword:
                # <group>.keyword[@<scope>] format => loop over all rids in group
                group = keyword.split(".")[0]
                if group in self.default_status_groups:
                    for rid in [rid for rid in self.config.sections() if rid.startswith(group+"#")]:
                        keyword = rid + keyword[keyword.index("."):]
                        changes.append(self.set_mangle(keyword, op, value, index))
                else:
                    # <section>.keyword[@<scope>]
                    changes.append(self.set_mangle(keyword, op, value, index))
            else:
                # <rid>.keyword[@<scope>]
                changes.append(self.set_mangle(keyword, op, value, index))
        self._set_multi(changes)

    def set_mono(self):
        self.set_multi_cache = {}
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        if self.options.value is None and \
           self.options.add is None and \
           self.options.remove is None:
            print("no value. set --value, --add or --remove", file=sys.stderr)
            return 1
        if self.options.add:
            op = "add"
            value = self.options.add
        elif self.options.remove:
            op = "remove"
            value = self.options.remove
        else:
            op = "set"
            value = self.options.value
        keyword = self.options.param
        index = self.options.index
        changes = []
        if "." in keyword and "#" not in keyword:
            # <group>.keyword[@<scope>] format => loop over all rids in group
            group = keyword.split(".")[0]
            if group in self.default_status_groups:
                for rid in [rid for rid in self.config.sections() if rid.startswith(group+"#")]:
                    keyword = rid + keyword[keyword.index("."):]
                    changes.append(self.set_mangle(keyword, op, value, index))
            else:
                # <section>.keyword[@<scope>]
                changes.append(self.set_mangle(keyword, op, value, index))
        else:
            # <rid>.keyword[@<scope>]
            changes.append(self.set_mangle(keyword, op, value, index))

        self._set_multi(changes)

    def set_mangle(self, keyword, op, value, index):
        def list_value(keyword):
            import rcConfigParser
            if keyword in self.set_multi_cache:
                return self.set_multi_cache[keyword].split()
            try:
                _value = self._get(keyword, self.options.eval).split()
            except (ex.excError, rcConfigParser.NoOptionError) as exc:
                _value = []
            return _value

        if op == "remove":
            _value = list_value(keyword)
            if value not in _value:
                return
            _value.remove(value)
            _value = " ".join(_value)
            self.set_multi_cache[keyword] = _value
        elif op == "add":
            _value = list_value(keyword)
            if value in _value:
                return
            index = index if index is not None else len(_value)
            _value.insert(index, value)
            _value = " ".join(_value)
            self.set_multi_cache[keyword] = _value
        else:
            _value = value
        elements = keyword.split('.')
        if self.has_default_section and len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            raise ex.excError("malformed kw: format as 'section.key'")
        return elements[0], elements[1], _value

    def _set(self, section, option, value):
        self._set_multi([[section, option, value]])

    def _set_multi(self, changes):
        changed = False
        lines = self._read_cf().splitlines()
        for change in changes:
            if change is None:
                continue
            section, option, value = change
            lines = self.set_line(lines, section, option, value)
            changed = True
        if not changed:
            # all changes were None
            return
        try:
            self._write_cf(lines)
        except (IOError, OSError) as exc:
            raise ex.excError(str(exc))
        unset_all_lazy(self)
        if hasattr(self, "ref_cache"):
            delattr(self, "ref_cache")

    def set_line(self, lines, section, option, value):
        """
        Set <option> to <value> in <section> of the configuration file.
        """
        section = "[%s]" % section
        done = False
        in_section = False
        value = try_decode(value)

        for idx, line in enumerate(lines):
            sline = line.strip()
            if sline == section:
                in_section = True
            elif in_section:
                if sline.startswith("["):
                    if done:
                        # matching section done and new section begins
                        break
                    else:
                        # section found and parsed and no option => add option
                        section_idx = idx
                        while section_idx > 0 and lines[section_idx-1].strip() == "":
                            section_idx -= 1
                        lines.insert(section_idx, "%s = %s" % (option, value))
                        done = True
                        break
                elif "=" in sline:
                    elements = sline.split("=")
                    _option = elements[0].strip()

                    if option != _option:
                        continue

                    if done:
                        # option already set : remove dup
                        del lines[idx]
                        while idx < len(lines) and "=" not in lines[idx] and \
                              not lines[idx].strip().startswith("[") and \
                              lines[idx].strip() != "":
                            del lines[idx]
                        continue

                    _value = elements[1].strip()
                    section_idx = idx

                    while section_idx < len(lines)-1 and  \
                          "=" not in lines[section_idx+1] and \
                          not lines[section_idx+1].strip().startswith("["):
                        section_idx += 1
                        if lines[section_idx].strip() == "":
                            continue
                        _value += " %s" % lines[section_idx].strip()

                    if value.replace("\n", " ") == _value:
                        return lines

                    lines[idx] = "%s = %s" % (option, value)
                    section_idx = idx

                    while section_idx < len(lines)-1 and \
                          "=" not in lines[section_idx+1] and \
                          not lines[section_idx+1].strip().startswith("[") and \
                          lines[section_idx+1].strip() != "":
                        del lines[section_idx+1]

                    done = True

        if not done:
            while len(lines) > 0 and lines[-1].strip() == "":
                lines.pop()
            if not in_section:
                # section in last position and no option => add section
                lines.append("")
                lines.append(section)
            lines.append("%s = %s" % (option, value))

        return lines

    #########################################################################
    #
    # config helpers
    #
    #########################################################################
    def handle_reference(self, ref, scope=False, impersonate=None, config=None):
            if "[" in ref and ref.endswith("]"):
                i = ref.index("[")
                index = ref[i+1:-1]
                ref = ref[:i]
                index = int(self.handle_references(index, scope=scope,
                                                   impersonate=impersonate))
            else:
                index = None

            if ref[0] == "#":
                return_length = True
                _ref = ref[1:]
            else:
                return_length = False
                _ref = ref

            # hardcoded references
            if _ref == "nodename":
                val = rcEnv.nodename
            elif _ref == "short_nodename":
                val = rcEnv.nodename.split(".")[0]
            elif _ref == "svcname" and hasattr(self, "svcname"):
                val = self.svcname
            elif _ref == "short_svcname" and hasattr(self, "svcname"):
                val = self.svcname.split(".")[0]
            elif _ref == "clustername":
                if hasattr(self, "node"):
                    val = self.node.cluster_name
                else:
                    val = self.cluster_name
            elif _ref == "clusternodes":
                if hasattr(self, "node"):
                    val = " ".join(self.node.cluster_nodes)
                else:
                    val = " ".join(self.cluster_nodes)
            elif _ref == "clusterdrpnodes":
                if hasattr(self, "node"):
                    val = " ".join(self.node.cluster_drpnodes)
                else:
                    val = " ".join(self.cluster_drpnodes)
            elif _ref == "dns":
                if hasattr(self, "node"):
                    val = " ".join(self.node.dns)
                else:
                    val = " ".join(self.dns)
            elif _ref == "dnsnodes":
                if hasattr(self, "node"):
                    val = " ".join(self.node.dnsnodes)
                else:
                    val = " ".join(self.dnsnodes)
            elif _ref == "svcmgr":
                val = rcEnv.paths.svcmgr
            elif _ref == "nodemgr":
                val = rcEnv.paths.nodemgr
            elif _ref == "etc":
                val = rcEnv.paths.pathetc
            elif _ref == "var":
                val = rcEnv.paths.pathvar
            elif _ref == "dnsuxsockd":
                val = rcEnv.paths.dnsuxsockd
            elif _ref == "dnsuxsock":
                val = rcEnv.paths.dnsuxsock
            else:
                val = None

            if val is None:
                # use DEFAULT as the implicit section
                n_dots = ref.count(".")
                if n_dots == 0 and self.has_default_section:
                    _section = "DEFAULT"
                    _v = ref
                elif n_dots == 1:
                    _section, _v = ref.split(".")
                else:
                    raise ex.excError("%s: reference can have only one dot" % ref)

                if len(_section) == 0:
                    raise ex.excError("%s: reference section can not be empty" % ref)
                if len(_v) == 0:
                    raise ex.excError("%s: reference option can not be empty" % ref)

                if _v[0] == "#":
                    return_length = True
                    _v = _v[1:]
                else:
                    return_length = False

                val = self._handle_reference(ref, _section, _v, scope=scope,
                                             impersonate=impersonate,
                                             config=config)

                if val is None:
                    # deferred
                    return

            if return_length or index is not None:
                if is_string(val):
                    val = val.split()
                if return_length:
                    return str(len(val))
                if index is not None:
                    try:
                        return val[index]
                    except IndexError:
                        if _v in ("exposed_devs", "sub_devs", "base_devs"):
                            return
                        raise

            return val

    def _handle_reference(self, ref, _section, _v, scope=False,
                          impersonate=None, config=None):
        if config is None:
            config = self.config
        # give os env precedence over the env cf section
        if _section == "env" and _v.upper() in os.environ:
            return os.environ[_v.upper()]

        if _section == "node" and hasattr(self, "svcname"):
            # go fetch the reference in the node.conf [node] section
            if self.node is None:
                from node import Node
                self.node = Node()
            try:
                return self.node.config.get("node", _v)
            except Exception as exc:
                raise ex.excError("%s: unresolved reference (%s)"
                                  "" % (ref, str(exc)))

        if _section != "DEFAULT" and not config.has_section(_section):
            raise ex.excError("%s: section %s does not exist" % (ref, _section))

        # deferrable refs
        if hasattr(self, "svcname"):
            for dref in ("exposed_devs", "base_devs", "sub_devs"):
                if _v != dref:
                    continue
                try:
                    res = self.get_resource(_section)
                    devs = getattr(res, dref)()
                    return list(devs)
                except Exception as exc:
                    return

        try:
            return self.conf_get(_section, _v, "string", scope=scope,
                                 impersonate=impersonate, config=config)
        except ex.OptNotFound as exc:
            return exc.default
        except ex.RequiredOptNotFound:
            raise ex.excError("%s: unresolved reference (%s)"
                              "" % (ref, str(exc)))

        raise ex.excError("%s: unknown reference" % ref)

    def _handle_references(self, s, scope=False, impersonate=None, config=None):
        if not is_string(s):
            return s
        while True:
            m = re.search(r'{\w*[\w#][\w\.\[\]]*}', s)
            if m is None:
                return s
            ref = m.group(0).strip("{}").lower()
            val = self.handle_reference(ref, scope=scope,
                                        impersonate=impersonate,
                                        config=config)
            if val is None:
                # deferred
                return
            s = s[:m.start()] + val + s[m.end():]

    @staticmethod
    def _handle_expressions(s):
        if not is_string(s):
            return s
        while True:
            m = re.search(r'\$\((.+)\)', s)
            if m is None:
                return s
            expr = m.group(1)
            try:
                val = eval_expr(expr)
            except TypeError as exc:
                raise ex.excError("invalid expression: %s: %s" % (expr, str(exc)))
            if m.start() == 0 and m.end() == len(s):
                # preserve the expression type
                return val
            s = s[:m.start()] + str(val) + s[m.end():]
            return s

    def handle_references(self, s, scope=False, impersonate=None, config=None):
        key = (s, scope, impersonate)
        if hasattr(self, "ref_cache") and self.ref_cache is not None \
           and key in self.ref_cache:
            return self.ref_cache[key]
        try:
            val = self._handle_references(s, scope=scope,
                                          impersonate=impersonate,
                                          config=config)
            val = self._handle_expressions(val)
            val = self._handle_references(val, scope=scope,
                                          impersonate=impersonate,
                                          config=config)
        except Exception as e:
            raise
            raise ex.excError("%s: reference evaluation failed: %s"
                              "" % (s, str(e)))
        if hasattr(self, "ref_cache") and self.ref_cache is not None and \
           val is not None:
            # don't cache lazy reference miss-evaluations
            self.ref_cache[key] = val
        return val

    def conf_get(self, s, o, t=None, scope=None, impersonate=None,
                 use_default=True, config=None, verbose=True):
        """
        Handle keyword and section deprecation.
        """
        section = s.split("#")[0]
        if section in self.kwdict.DEPRECATED_SECTIONS:
            section, rtype = self.kwdict.DEPRECATED_SECTIONS[section]
            fkey = ".".join((section, rtype, o))
        else:
            try:
                rtype = self.config.get(s, "type")
                fkey = ".".join((section, rtype, o))
            except Exception:
                if hasattr(self, "svcname") and section == "sync":
                    rtype = "rsync"
                    fkey = ".".join((section, rtype, o))
                else:
                    rtype = None
                    fkey = ".".join((section, o))

        deprecated_keywords = self.kwdict.REVERSE_DEPRECATED_KEYWORDS.get(fkey)
        if deprecated_keywords is not None and not isinstance(deprecated_keywords, list):
            deprecated_keywords = [deprecated_keywords]

        # 1st try: supported keyword
        try:
            return self._conf_get(s, o, t=t, scope=scope,
                                  impersonate=impersonate,
                                  use_default=use_default, config=config,
                                  section=section, rtype=rtype)
        except ex.RequiredOptNotFound:
            if deprecated_keywords is None:
                if verbose:
                    self.log.error("%s.%s is mandatory" % (s, o))
                raise
        except ex.OptNotFound:
            if deprecated_keywords is None:
                raise

        # 2nd try: deprecated keyword
        exc = None
        for deprecated_keyword in deprecated_keywords:
            try:
                return self._conf_get(s, deprecated_keyword, t=t, scope=scope,
                                      impersonate=impersonate,
                                      use_default=use_default, config=config,
                                      section=section, rtype=rtype)
            except ex.RequiredOptNotFound as exc:
                pass
        if exc:
            self.log.error("%s.%s is mandatory" % (s, o))
            raise exc

    def _conf_get(self, s, o, t=None, scope=None, impersonate=None,
                 use_default=True, config=None, section=None, rtype=None):
        """
        Get keyword properties and handle inheritance.
        """
        inheritance = "leaf"
        kwargs = {
            "default": None,
            "required": False,
            "deprecated": False,
            "impersonate": impersonate,
            "use_default": use_default,
            "config": config,
        }
        if s != "env":
            key = self.kwdict.KEYS[section].getkey(o, rtype)
            if key is None:
                if scope is None and t is None:
                    raise ValueError("%s.%s not found in the "
                                     "keywords dictionary" % (s, o))
                else:
                    # passing 't' and 'scope' skips KEYS validation.
                    # used for keywords not in KEYS.
                    pass
            else:
                kwargs["deprecated"] = (key.keyword != o)
                kwargs["required"] = key.required
                kwargs["default"] = key.default
                inheritance = key.inheritance
                if scope is None:
                    scope = key.at
                if t is None:
                    t = key.convert
        else:
            # env key are always string and scopable
            t = "string"
            scope = True

        if scope is None:
            scope = False
        if t is None:
            t = "string"

        kwargs["scope"] = scope
        kwargs["t"] = t

        # in order of probability
        if self.has_default_section:
            if inheritance == "leaf > head":
                return self.__conf_get(s, o, **kwargs)
            if inheritance == "leaf":
                kwargs["use_default"] = False
                return self.__conf_get(s, o, **kwargs)
            if inheritance == "head":
                return self.__conf_get("DEFAULT", o, **kwargs)
            if inheritance == "head > leaf":
                try:
                    return self.__conf_get("DEFAULT", o, **kwargs)
                except ex.OptNotFound:
                    kwargs["use_default"] = False
                    return self.__conf_get(s, o, **kwargs)
        else:
            return self.__conf_get(s, o, **kwargs)
        raise ex.excError("unknown inheritance value: %s" % str(inheritance))

    def __conf_get(self, s, o, t=None, scope=None, impersonate=None,
                   use_default=None, config=None, default=None, required=None,
                   deprecated=None):
        try:
            if not scope:
                val = self.conf_get_val_unscoped(s, o, use_default=use_default,
                                                 config=config)
            else:
                val = self.conf_get_val_scoped(s, o, use_default=use_default,
                                               config=config,
                                               impersonate=impersonate)
        except ex.OptNotFound as exc:
            if required:
                raise ex.RequiredOptNotFound
            else:
                exc.default = default
                raise exc

        try:
            val = self.handle_references(val, scope=scope,
                                         impersonate=impersonate,
                                         config=config)
        except ex.excError as exc:
            if o.startswith("pre_") or o.startswith("post_") or \
               o.startswith("blocking_"):
                pass
            else:
                raise

        if t in (None, "string"):
            return val
        return globals()["convert_"+t](val)

    def conf_get_val_unscoped(self, s, o, use_default=True, config=None):
        if config is None:
            config = self.config
        if config.has_option(s, o):
            return config.get(s, o)
        raise ex.OptNotFound("unscoped keyword %s.%s not found." % (s, o))

    def conf_has_option_scoped(self, s, o, impersonate=None, config=None, scope_order=None):
        """
        Handles the keyword scope_order property, at and impersonate
        """
        if config is None:
            config = self.config
        if impersonate is None:
            nodename = rcEnv.nodename
        else:
            nodename = impersonate

        if s != "DEFAULT" and not config.has_section(s):
            return

        if s == "DEFAULT":
            options = config.defaults().keys()
        else:
            options = config._sections[s].keys()

        candidates = [
            (o+"@"+nodename, True),
        ]
        if hasattr(self, "svcname"):
            candidates += [
                (o+"@nodes", nodename in self.nodes),
                (o+"@drpnodes", nodename in self.drpnodes),
                (o+"@encapnodes", nodename in self.encapnodes),
                (o+"@flex_primary", nodename == self.flex_primary),
                (o+"@drp_flex_primary", nodename == self.drp_flex_primary),
            ]
        else:
            candidates += [
                (o+"@nodes", nodename in self.cluster_nodes),
                (o+"@drpnodes", nodename in self.cluster_drpnodes),
            ]
        candidates += [
            (o, True),
        ]

        if scope_order == "head: generic > specific" and s == "DEFAULT":
            candidates.reverse()
        elif scope_order == "generic > specific":
            candidates.reverse()

        for option, condition in candidates:
            if option in options and condition:
                return option

    def conf_get_val_scoped(self, s, o, impersonate=None, use_default=True, config=None, scope_order=None):
        if config is None:
            config = self.config
        if impersonate is None:
            nodename = rcEnv.nodename
        else:
            nodename = impersonate

        option = self.conf_has_option_scoped(s, o, impersonate=impersonate,
                                             config=config,
                                             scope_order=scope_order)
        if option is None and (not self.has_default_section or not use_default):
            raise ex.OptNotFound("scoped keyword %s.%s not found." % (s, o))

        if option is None and use_default and self.has_default_section:
            if s != "DEFAULT":
                # fallback to default
                return self.conf_get_val_scoped("DEFAULT", o,
                                                impersonate=impersonate,
                                                config=config,
                                                scope_order=scope_order)
            else:
                raise ex.OptNotFound("scoped keyword %s.%s not found." % (s, o))

        try:
            val = config.get(s, option)
        except Exception as e:
            raise ex.excError("param %s.%s: %s"%(s, o, str(e)))

        return val

    def validate_config(self, path=None):
        """
        The validate config action entrypoint.
        """
        ret = self._validate_config(path=path)
        return ret["warnings"] + ret["errors"]

    def _validate_config(self, path=None):
        """
        The validate config core method.
        Returns a dict with the list of syntax warnings and errors.
        """
        try:
            import ConfigParser
        except ImportError:
            import configparser as ConfigParser

        ret = {
            "errors": 0,
            "warnings": 0,
        }

        if path is None:
            config = self.config
        else:
            try:
                config = read_cf(path)
            except ConfigParser.ParsingError:
                self.log.error("error parsing %s" % path)
                ret["errors"] += 1

        def check_scoping(key, section, option):
            """
            Verify the specified option scoping is allowed.
            """
            if not key.at and "@" in option:
                self.log.error("option %s.%s does not support scoping", section, option)
                return 1
            return 0

        def check_references(section, option):
            """
            Verify the specified option references.
            """
            value = config.get(section, option)
            try:
                value = self.handle_references(value, scope=True, config=config)
            except ex.excError as exc:
                if not option.startswith("pre_") and \
                   not option.startswith("post_") and \
                   not option.startswith("blocking_"):
                    self.log.error(str(exc))
                    return 1
            except Exception as exc:
                self.log.error(str(exc))
                return 1
            return 0

        def get_val(key, section, option):
            """
            Fetch the value and convert it to the expected type.
            """
            _option = option.split("@")[0]
            value = self.conf_get(section, _option, config=config)
            return value

        def check_candidates(key, section, option, value):
            """
            Verify the specified option value is in allowed candidates.
            """
            if not key.strict_candidates:
                return 0
            if key.candidates is None:
                return 0
            if isinstance(value, (list, tuple, set)):
                valid = len(set(value) - set(key.candidates)) == 0
            else:
                valid = value in key.candidates
            if not valid:
                if isinstance(key.candidates, (set, list, tuple)):
                    candidates = ", ".join([str(candidate) for candidate in key.candidates])
                else:
                    candidates = str(key.candidates)
                self.log.error("option %s.%s value %s is not in valid candidates: %s",
                               section, option, str(value), candidates)
                return 1
            return 0

        def check_known_option(key, section, option):
            """
            Verify the specified option scoping, references and that the value
            is in allowed candidates.
            """
            err = 0
            err += check_scoping(key, section, option)
            if check_references(section, option) != 0:
                err += 1
                return err
            try:
                value = get_val(key, section, option)
            except ValueError as exc:
                self.log.warning(str(exc))
                return 0
            except ex.OptNotFound:
                return 0
            err += check_candidates(key, section, option, value)
            return err

        def validate_default_options(config, ret):
            """
            Validate DEFAULT section options.
            """
            if not self.has_default_section:
                return ret
            for option in config.defaults():
                key = self.kwdict.KEYS.sections["DEFAULT"].getkey(option)
                if key is None:
                    found = False
                    # the option can be set in the DEFAULT section for the
                    # benefit of a resource section
                    for section in config.sections():
                        family = section.split("#")[0]
                        if family not in list(self.kwdict.KEYS.sections.keys()) + \
                           list(self.kwdict.DEPRECATED_SECTIONS.keys()):
                            continue
                        if family in self.kwdict.DEPRECATED_SECTIONS:
                            results = self.kwdict.DEPRECATED_SECTIONS[family]
                            family = results[0]
                        if self.kwdict.KEYS.sections[family].getkey(option) is not None:
                            found = True
                            break
                    if not found:
                        self.log.warning("ignored option DEFAULT.%s", option)
                        ret["warnings"] += 1
                else:
                    # here we know its a native DEFAULT option
                    ret["errors"] += check_known_option(key, "DEFAULT", option)
            return ret

        def validate_resources_options(config, ret):
            """
            Validate resource sections options.
            """
            for section in config.sections():
                if section == "env":
                    # the "env" section is not handled by a resource driver, and is
                    # unknown to the self.kwdict. Just ignore it.
                    continue
                family = section.split("#")[0]
                if config.has_option(section, "type"):
                    rtype = config.get(section, "type")
                else:
                    rtype = None
                if family not in list(self.kwdict.KEYS.sections.keys()) + list(self.kwdict.DEPRECATED_SECTIONS.keys()):
                    self.log.warning("ignored section %s", section)
                    ret["warnings"] += 1
                    continue
                if family in self.kwdict.DEPRECATED_SECTIONS:
                    self.log.warning("deprecated section prefix %s", family)
                    ret["warnings"] += 1
                    family, rtype = self.kwdict.DEPRECATED_SECTIONS[family]
                for option in config.options(section):
                    if option in config.defaults():
                        continue
                    key = self.kwdict.KEYS.sections[family].getkey(option, rtype=rtype)
                    if key is None:
                        key = self.kwdict.KEYS.sections[family].getkey(option)
                    if key is None:
                        self.log.warning("ignored option %s.%s%s", section,
                                         option, ", driver %s" % rtype if rtype else "")
                        ret["warnings"] += 1
                    else:
                        ret["errors"] += check_known_option(key, section, option)
            return ret

        def validate_build(path, ret):
            """
            Try a service build to catch errors missed in other tests.
            """
            if not hasattr(self, "svcname"):
                return ret
            from svcBuilder import build
            try:
                build(self.svcname, svcconf=path, node=self.node)
            except Exception as exc:
                self.log.error("the new configuration causes the following "
                               "build error: %s", str(exc))
                ret["errors"] += 1
            return ret

        ret = validate_default_options(config, ret)
        ret = validate_resources_options(config, ret)
        ret = validate_build(path, ret)

        return ret

    def _read_cf(self):
        """
        Return the service config file content.
        """
        if not os.path.exists(self.paths.cf):
            return ""
        import codecs
        with codecs.open(self.paths.cf, "r", "utf8") as ofile:
            buff = ofile.read()
        return buff

    def _write_cf(self, buff):
        """
        Truncate the service config file and write buff.
        """
        import codecs
        import tempfile
        import shutil
        if isinstance(buff, list):
            buff = "\n".join(buff) + "\n"
        if hasattr(self, "svcname"):
            prefix = self.svcname
        else:
            prefix = "node"
        ofile = tempfile.NamedTemporaryFile(delete=False, dir=rcEnv.paths.pathtmp, prefix=prefix)
        fpath = ofile.name
        os.chmod(fpath, 0o0640)
        ofile.close()
        with codecs.open(fpath, "w", "utf8") as ofile:
            ofile.write(buff)
            ofile.flush()
        shutil.move(fpath, self.paths.cf)

    def print_config_data(self, src_config=None, evaluate=False, impersonate=None):
        """
        Return a simple dict (OrderedDict if possible), fed with the
        service configuration sections and keys
        """
        try:
            from collections import OrderedDict
            best_dict = OrderedDict
        except ImportError:
            best_dict = dict
        data = best_dict()
        tmp = best_dict()
        if src_config is None:
            config = self.config
        else:
            config = src_config

        defaults = config.defaults()
        for key in defaults.keys():
            tmp[key] = defaults[key]

        data['DEFAULT'] = tmp
        config._defaults = {}

        sections = config.sections()
        for section in sections:
            options = config.options(section)
            tmpsection = best_dict()
            for option in options:
                if config.has_option(section, option):
                    tmpsection[option] = config.get(section, option)
            data[section] = tmpsection
        if src_config is None:
            unset_lazy(self, "config")
        if not evaluate:
            return data
        edata = {}
        for section, _data in data.items():
            edata[section] = {}
            keys = []
            for key in _data:
                key = key.split("@")[0]
                if key in edata[section]:
                    continue
                val = self.conf_get(section, key, impersonate=impersonate)
                # ensure the data is json-exportable
                if isinstance(val, set):
                    val = list(val)
                edata[section][key] = val
        return edata

