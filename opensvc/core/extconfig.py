from __future__ import print_function

import ast
import codecs
import copy
import operator as op
import os
import re
import sys

import foreign.six as six

import core.exceptions as ex
from env import Env
from utilities.naming import factory
from utilities.files import makedirs
from utilities.configparser import RawConfigParser, NoOptionError
from utilities.converters import *
from utilities.lazy import lazy
from utilities.string import is_string, try_decode

SECRETS = []

DEFER = [
    (None, "exposed_devs"),
    (None, "sub_devs"),
    (None, "base_devs"),
    ("volume", "mnt"),
]

# supported operators in arithmetic expressions
operators = {
    ast.Add: op.add,
    ast.Sub: op.sub,
    ast.Mult: op.mul,
    ast.Div: op.truediv,
    ast.Pow: op.pow,
    ast.BitOr: op.or_,
    ast.BitAnd: op.and_,
    ast.BitXor: op.xor,
    ast.USub: op.neg,
    ast.FloorDiv: op.floordiv,
    ast.Mod: op.mod,
    ast.Not: op.not_,
    ast.Eq: op.eq,
    ast.NotEq: op.ne,
    ast.Lt: op.lt,
    ast.LtE: op.le,
    ast.Gt: op.gt,
    ast.GtE: op.ge,
    ast.In: op.contains,
}


def eval_expr(expr):
    """ arithmetic expressions evaluator
    """

    def eval_(node):
        _safe_names = {'None': None, 'True': True, 'False': False}
        if isinstance(node, ast.Num):  # <number>
            return node.n
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Name):
            if node.id in _safe_names:
                return _safe_names[node.id]
            return node.id
        elif isinstance(node, ast.Tuple):
            return tuple(node.elts)
        elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
            return operators[type(node.op)](eval_(node.left), eval_(node.right))
        elif isinstance(node, ast.UnaryOp):  # <operator> <operand> e.g., -1
            return operators[type(node.op)](eval_(node.operand))
        elif isinstance(node, ast.BoolOp):  # Boolean operator: either "and" or "or" with two or more values
            if type(node.op) == ast.And:
                return all(eval_(val) for val in node.values)
            else:  # Or:
                for val in node.values:
                    result = eval_(val)
                    if result:
                        return result
                    return result  # or returns the final value even if it's falsy
        elif isinstance(node, ast.Compare):  # A comparison expression, e.g. "3 > 2" or "5 < x < 10"
            left = eval_(node.left)
            for comparison_op, right_expr in zip(node.ops, node.comparators):
                right = eval_(right_expr)
                if type(comparison_op) == ast.In:
                    if isinstance(right, tuple):
                        if not any(q.id == left for q in right if isinstance(q, ast.Name)):
                            return False
                    else:
                        if not operators[type(comparison_op)](right, left):
                            return False
                else:
                    if not operators[type(comparison_op)](left, right):
                        return False
                left = right
                return True
        elif isinstance(node, ast.Attribute):
            raise TypeError("strings with dots need quoting")
        elif hasattr(ast, "NameConstant") and isinstance(node, getattr(ast, "NameConstant")):
            return node.value
        else:
            raise TypeError("unsupported node type %s" % type(node))

    return eval_(ast.parse(expr, mode='eval').body)


def read_cf(fpaths, defaults=None):
    """
    Read and parse an arbitrary ini-formatted config file, and return
    the RawConfigParser object.
    """
    try:
        from collections import OrderedDict
        config = RawConfigParser(dict_type=OrderedDict)
    except ImportError:
        config = RawConfigParser()

    if defaults is None:
        defaults = {}
    config = RawConfigParser(defaults)
    config.optionxform = str
    if not isinstance(fpaths, (list, tuple)):
        fpaths = [fpaths]
    for fpath in fpaths:
        if not os.path.exists(fpath):
            continue
        with codecs.open(fpath, "r", "utf8") as ofile:
            try:
                if six.PY3:
                    config.read_file(ofile)
                else:
                    config.readfp(ofile)
            except AttributeError:
                raise
    return config


def read_cf_comments(fpath):
    data = {}
    if isinstance(fpath, list):
        return data
    if not os.path.exists(fpath):
        return data
    section = ".header"
    current = []

    with codecs.open(fpath, "r", "utf8") as ofile:
        buff = ofile.read()

    for line in buff.splitlines():
        line = line.strip()
        if not line:
            continue
        if re.match(r"\[.+\]", line):
            if current:
                data[section] = current
                current = []
            section = line[1:-1]
            continue
        if line[0] in (";", "#"):
            stripped = line.lstrip("#;").strip()
            if re.match(r"\[.+\]", stripped):
                # add an empty line before a commented section
                current.append("")
            current.append(stripped)
    if current:
        data[section] = current
        current = []
    return data


class ExtConfigMixin(object):
    def __init__(self, default_status_groups=None):
        self.ref_cache = {}
        self.default_status_groups = default_status_groups

    def clear_ref_cache(self):
        self.ref_cache = {}

    @lazy
    def has_default_section(self):
        if hasattr(self, "path"):
            return True
        else:
            return False

    def delete_sections(self, sections=None):
        """
        Delete config file sections.
        """
        if sections is None:
            sections = []
        try:
            cd = self.private_cd
        except AttributeError:
            cd = self.cd
        deleted = 0
        for section in sections:
            try:
                del cd[section]
                deleted += 1
            except KeyError:
                self.log.info("skip delete %s: not found", section)
        if deleted:
            self.commit()

    def unset(self):
        """
        The 'unset' action entrypoint.
        Verifies the --param is set, and finally call the _unset
        internal method.
        """
        if self.options.kw:
            kws = self.options.kw
        elif self.options.param:
            kws = [self.options.param]
        else:
            print("no keyword specified. set --kw <keyword>", file=sys.stderr)
            return 1
        self.unset_multi(kws)

    def unset_multi(self, kws):
        try:
            cd = self.private_cd
        except AttributeError:
            cd = self.cd
        if not kws:
            return
        deleted = 0
        for kw in kws:
            try:
                section, option = kw.split(".", 1)
            except Exception:
                section = "DEFAULT"
                option = kw
            if 'DEFAULT' not in section:
                section = section.lower()
            option = option.lower()
            try:
                del cd[section][option]
                deleted += 1
            except KeyError:
                continue
        if not deleted:
            return 0
        try:
            self.commit(cd=cd)
        except (IOError, OSError) as exc:
            raise ex.Error(str(exc))
        return deleted

    def eval(self):
        """
        The 'eval' action entrypoint.
        Verifies --kw is set, set DEFAULT as section if no section was
        specified, and finally print the dereferenced and evaluated
        value.
        """
        if self.options.kw and len(self.options.kw) == 1:
            kw = self.options.kw[0]
        else:
            kw = None
        try:
            print(self._get(kw, evaluate=True, impersonate=self.options.impersonate))
        except ex.OptNotFound as exc:
            print(exc.default)
        except ex.RequiredOptNotFound as exc:
            return 1
        except ex.Error as exc:
            print(exc, file=sys.stderr)
            return 1
        except Exception:
            return 1
        return 0

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
            print(self._get(kw, evaluate=self.options.eval, impersonate=self.options.impersonate))
        except ex.OptNotFound as exc:
            print(exc.default)
        except ex.RequiredOptNotFound as exc:
            return 1
        except ex.Error as exc:
            print(exc, file=sys.stderr)
            return 1
        except Exception:
            return 1
        return 0

    def _get(self, param=None, evaluate=False, impersonate=None):
        """
        Verifies the param is set, set DEFAULT as section if no section was
        specified, and finally return,
        * the raw value if evaluate is False
        * the dereferenced and evaluated value if evaluate is True
        """
        if param is None:
            raise ex.Error("no parameter. set --param")
        elements = param.split(".", 1)
        if self.has_default_section and len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            raise ex.Error("malformed parameter. format as 'section.key'")
        section, option = elements
        if section == "DEFAULT" and not self.has_default_section:
            raise ex.Error("the DEFAULT section is not allowed in %s" % self.paths.cf)
        if section not in self.cd:
            if section != 'DEFAULT' and self.has_default_section:
                raise ex.OptNotFound("section [%s] not found" % section)
            if not self.has_default_section:
                raise ex.OptNotFound("section [%s] not found" % section)
        if evaluate:
            if self.options.format is None:
                self.options.format = "json"
            return self.conf_get(section, option, scope=True, impersonate=impersonate)
        else:
            try:
                return self.cd[section][option]
            except Exception:
                raise ex.OptNotFound()

    def set(self):
        """
        The 'set' action entrypoint.
        Verifies the --param and --value are set, set DEFAULT as section
        if no section was specified, and set the value using the internal
        _set() method.
        """
        if self.options.eval:
            eval = self.options.eval
        else:
            eval = False
        if self.options.kw is not None:
            return self.set_multi(self.options.kw, eval=eval)
        else:
            return self.set_mono(eval=eval)

    def set_multi(self, kws, eval=False, validation=True):
        try:
            cd = self.private_cd
        except AttributeError:
            cd = self.cd
        changes = []
        self.set_multi_cache = {}
        for kw in kws:
            if "=" not in kw:
                raise ex.Error("malformed kw expression: %s: no '='" % kw)
            keyword, value = kw.split("=", 1)
            if 'DEFAULT' not in keyword and not keyword.startswith("data."):
                keyword = keyword.lower()
            if keyword[-1] == "-":
                op = "remove"
                keyword = keyword[:-1]
            elif keyword[-1] == "|":
                op = "add"
                keyword = keyword[:-1]
            elif keyword[-1] == "+":
                op = "insert"
                keyword = keyword[:-1]
            else:
                op = "set"
            index = None
            if "[" in keyword:
                keyword, right = keyword.split("[", 1)
                if not right.endswith("]"):
                    raise ex.Error("malformed kw expression: %s: no trailing"
                                   " ']' at the end of keyword" % kw)
                try:
                    index = int(right[:-1])
                except ValueError:
                    raise ex.Error("malformed kw expression: %s: index is "
                                   "not integer" % kw)
            if "." in keyword and "#" not in keyword:
                # <group>.keyword[@<scope>] format => loop over all rids in group
                group = keyword.split(".", 1)[0]
                if group in self.default_status_groups:
                    for rid in [rid for rid in cd if rid.startswith(group+"#")]:
                        keyword = rid + keyword[keyword.index("."):]
                        changes.append(self.set_mangle(keyword, op, value, index, eval))
                else:
                    # <section>.keyword[@<scope>]
                    changes.append(self.set_mangle(keyword, op, value, index, eval))
            else:
                # <rid>.keyword[@<scope>]
                changes.append(self.set_mangle(keyword, op, value, index, eval))
        self._set_multi(changes, validation=validation)

    def set_mono(self, eval=False):
        try:
            cd = self.private_cd
        except AttributeError:
            cd = self.cd
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
        keyword = self.options.param.lower()
        index = self.options.index
        changes = []
        if "." in keyword and "#" not in keyword:
            # <group>.keyword[@<scope>] format => loop over all rids in group
            group = keyword.split(".", 1)[0]
            if group in self.default_status_groups:
                for rid in [rid for rid in cd if rid.startswith(group+"#")]:
                    keyword = rid + keyword[keyword.index("."):]
                    changes.append(self.set_mangle(keyword, op, value, index, eval))
            else:
                # <section>.keyword[@<scope>]
                changes.append(self.set_mangle(keyword, op, value, index, eval))
        else:
            # <rid>.keyword[@<scope>]
            changes.append(self.set_mangle(keyword, op, value, index, eval))

        self._set_multi(changes)

    def set_mangle(self, keyword, op, value, index, eval):
        def list_value(keyword):
            if keyword in self.set_multi_cache:
                return self.set_multi_cache[keyword].split()
            try:
                _value = self._get(keyword, eval).split()
            except (ex.Error, NoOptionError) as exc:
                _value = []
            except ex.OptNotFound as exc:
                _value = copy.copy(exc.default)
            if _value is None:
                _value = []
            return _value

        if op == "remove":
            _value = list_value(keyword)
            if value not in _value:
                return
            _value.remove(value)
            _value = " ".join(_value)
            self.set_multi_cache[keyword] = _value
        elif op == "insert":
            _value = list_value(keyword)
            if index is None:
                i = len(_value)
            else:
                i = index
            _value.insert(i, value)
            _value = " ".join(_value)
            self.set_multi_cache[keyword] = _value
        elif op == "add":
            _value = list_value(keyword)
            for v in value.split():
                if v in _value:
                    continue
                if index is None:
                    i = len(_value)
                else:
                    i = index
                    index += 1
                _value.insert(i, v)
            _value = " ".join(_value)
            self.set_multi_cache[keyword] = _value
        else:
            _value = value
        elements = keyword.split(".", 1)
        if self.has_default_section and len(elements) == 1:
            elements.insert(0, "DEFAULT")
        elif len(elements) != 2:
            raise ex.Error("malformed kw: format as 'section.key'")
        return elements[0], elements[1], _value, eval

    def _set(self, section, option, value, validation=True):
        changes = [(section, option, value, False)]
        self._set_multi(changes, validation=validation)

    def _set_one(self, section, option, value):
        value = try_decode(value)
        try:
            cd = self.private_cd
        except AttributeError:
            cd = self.cd
        changed = False
        if section not in cd:
            cd[section] = {}
            changed = True
        try:
            current = cd[section][option]
            if current != value:
                cd[section][option] = value
                changed = True
        except KeyError:
            cd[section][option] = value
            changed = True
        return changed

    def _set_multi(self, changes, validation=True):
        changed = False
        for change in changes:
            if change is None:
                continue
            section, option, value, eval = change
            changed |= self._set_one(section, option, value)
            changed = True
        if not changed:
            return
        try:
            self.commit(validation=validation)
        except (IOError, OSError) as exc:
            raise ex.Error(str(exc))

    #########################################################################
    #
    # config helpers
    #
    #########################################################################
    def handle_reference(self, ref, scope=False, impersonate=None, cd=None,
                         section=None):
        if "[" in ref and ref.endswith("]"):
            i = ref.index("[")
            index = ref[i+1:-1]
            ref = ref[:i]
            index = int(self.handle_references(index, scope=scope,
                                               impersonate=impersonate,
                                               section=section))
        else:
            index = None

        if ref[0] == "#":
            return_length = True
            _ref = ref[1:]
        else:
            return_length = False
            _ref = ref

        is_svc = hasattr(self, "path")
        has_node = hasattr(self, "node")

        modifier = None
        if _ref.startswith("upper:"):
            _ref = _ref[6:]
            modifier = lambda x: x.upper()
        elif _ref.startswith("lower:"):
            _ref = _ref[6:]
            modifier = lambda x: x.lower()
        elif _ref.startswith("capitalize:"):
            _ref = _ref[11:]
            modifier = lambda x: x.capitalize()
        elif _ref.startswith("title:"):
            _ref = _ref[6:]
            modifier = lambda x: x.title()
        elif _ref.startswith("swapcase:"):
            _ref = _ref[9:]
            modifier = lambda x: x.swapcase()
        elif _ref.startswith("uri_ip:"):
            _ref = _ref[7:]
            modifier = lambda x: "[%s]" % x if ":" in str(x) else x

        # hardcoded references
        if _ref == "nodename":
            val = Env.nodename
        elif _ref == "short_nodename":
            val = Env.nodename.split(".")[0]
        elif _ref == "namespace" and is_svc:
            val = self.namespace if self.namespace else "root"
        elif _ref == "kind" and is_svc:
            val = self.kind
        elif _ref == "id" and is_svc:
            val = self.id
        elif _ref in ("path", "svcpath") and is_svc:
            val = self.path
        elif _ref in ("name", "svcname") and is_svc:
            val = self.name
        elif _ref in ("short_name", "short_svcname") and is_svc:
            val = self.name.split(".")[0]
        elif _ref in ("scaler_name", "scaler_svcname") and is_svc:
            val = re.sub(r"[0-9]+\.", "", self.name)
        elif _ref in ("scaler_short_name", "scaler_short_svcname") and is_svc:
            val = re.sub(r"[0-9]+\.", "", self.name.split(".")[0])
        elif _ref == "rid" and is_svc:
            val = section
        elif _ref == "rindex" and is_svc:
            val = section.split("#")[-1]
        elif _ref == "clusterid":
            if has_node:
                val = self.node.cluster_id
            else:
                val = self.cluster_id
        elif _ref == "clustername":
            if has_node:
                val = self.node.cluster_name
            else:
                val = self.cluster_name
        elif _ref == "fqdn":
            if has_node:
                ns = "root" if self.namespace is None else self.namespace
                val = "%s.%s.%s.%s" % (self.name, ns, self.kind, self.node.cluster_name)
        elif _ref == "domain":
            if has_node:
                val = "%s.%s.%s" % (self.namespace, self.kind, self.node.cluster_name)
        elif _ref == "clusternodes":
            if has_node:
                val = " ".join(self.node.cluster_nodes)
            else:
                val = " ".join(self.cluster_nodes)
        elif _ref == "clusterdrpnodes":
            if has_node:
                val = " ".join(self.node.cluster_drpnodes)
            else:
                val = " ".join(self.cluster_drpnodes)
        elif _ref == "dns":
            if has_node:
                val = " ".join(self.node.dns)
            else:
                val = " ".join(self.dns)
        elif _ref == "dnsnodes":
            if has_node:
                val = " ".join(self.node.dnsnodes)
            else:
                val = " ".join(self.dnsnodes)
        elif _ref == "svcmgr":
            val = Env.paths.svcmgr
        elif _ref == "nodemgr":
            val = Env.paths.nodemgr
        elif _ref == "etc":
            val = Env.paths.pathetc
        elif _ref == "var":
            val = Env.paths.pathvar
        elif _ref == "private_var":
            val = self.var_d
        elif _ref == "collector_api":
            if has_node:
                url = self.node.collector_env.dbopensvc
            else:
                url = self.collector_env.dbopensvc
            if url:
                val = url.replace("/feed/default/call/xmlrpc", "/init/rest/api") if url else ""
            else:
                val = ""
        elif _ref == "dnsuxsockd":
            val = Env.paths.dnsuxsockd
        elif _ref == "dnsuxsock":
            val = Env.paths.dnsuxsock
        elif _ref.startswith("safe://"):
            try:
                if has_node:
                    val = self.node.download_from_safe(_ref, path=self.path)
                else:
                    val = self.download_from_safe(_ref)
                val = val.decode()
                SECRETS.append(val)
            except ex.Error as exc:
                val = ""
        else:
            val = None

        _v = None
        if val is None:
            # use DEFAULT as the implicit section
            n_dots = _ref.count(".")
            if n_dots == 0 and section and section != "DEFAULT":
                _section = section
                _v = _ref
            elif n_dots == 0 and self.has_default_section:
                _section = "DEFAULT"
                _v = _ref
            elif n_dots >= 1:
                _section, _v = _ref.split(".", 1)

            if len(_section) == 0:
                raise ex.Error("%s: reference section can not be empty" % _ref)
            if len(_v) == 0:
                raise ex.Error("%s: reference option can not be empty" % _ref)

            try:
                val = self._handle_reference(_ref, _section, _v, scope=scope,
                                             impersonate=impersonate,
                                             cd=cd, return_length=return_length)
            except Exception:
                val = None

            if val is None and _section != "DEFAULT" and n_dots == 0 and self.has_default_section:
                val = self._handle_reference(ref, "DEFAULT", _v, scope=scope,
                                             impersonate=impersonate, cd=cd,
                                             return_length=return_length)

            if val is None:
                # deferred
                return

        if return_length or index is not None:
            if is_string(val):
                val = val.split()
            if return_length:
                val = str(len(val))
            elif index is not None:
                try:
                    val = val[index]
                except IndexError:
                    drv_group = section.split("#", 1)[0] if section else None
                    if _v is not None and ((None, _v) in DEFER or (drv_group, _v) in DEFER):
                        return
                    val = ""

        if modifier and val:
            try:
                return modifier(val)
            except Exception as exc:
                pass
        return val

    def _handle_reference(self, ref, _section, _v, scope=False,
                          impersonate=None, cd=None, return_length=False):
        if cd is None:
            try:
                cd = self.private_cd
            except AttributeError:
                cd = self.cd
        # give os env precedence over the env cf section
        if _section == "env" and _v.upper() in os.environ:
            return os.environ[_v.upper()]

        if _section == "node" and hasattr(self, "path"):
            # go fetch the reference in the node.conf [node] section
            try:
                # set BaseSvc::node if not already set
                self.get_node()
                if "." in _v:
                    __section, __v = _v.split(".", 1)
                    if __section in ("env", "labels"):
                        # allowed explicit section
                        return self.node.conf_get(__section, __v)
                # use "node" as the implicit section
                return self.node.conf_get("node", _v)
            except Exception as exc:
                raise ex.Error("%s: unresolved reference (%s)" % (ref, str(exc)))

        if _section != "DEFAULT" and _section not in cd:
            raise ex.Error("%s: section %s does not exist" % (ref, _section))

        # deferrable refs
        if hasattr(self, "path"):
            drv_group = _section.split("#", 1)[0]
            if (drv_group, _v) in DEFER or (None, _v) in DEFER:
                try:
                    self.init_resources()
                    res = self.resources_by_id[_section]
                    fn = getattr(res, _v)
                    result = fn()
                    if isinstance(result, set):
                        return list(result)
                    else:
                        return result
                except Exception as exc:
                    return

        try:
            t = None if return_length else "string"
            return self.conf_get(_section, _v, t, scope=scope,
                                 impersonate=impersonate, cd=cd)
        except ex.OptNotFound as exc:
            return copy.copy(exc.default)
        except ex.RequiredOptNotFound as exc:
            raise ex.Error("%s: unresolved reference (%s)" % (ref, str(exc)))

        raise ex.Error("%s: unknown reference" % ref)

    def _handle_references(self, s, scope=False, impersonate=None, cd=None,
                           section=None, first_step=None):
        if not is_string(s):
            return s
        done = ""
        while True:
            m = re.search(r'{\w*[\w#][\w\.\[\]:\/]*}', s)
            if m is None:
                return done + s
            ref = m.group(0).strip("{}").lower()
            if first_step and ref.startswith("safe://"):
                # do safe references after expressions only
                done += s[:m.end()]
                s = s[m.end():]
                continue
            val = self.handle_reference(ref, scope=scope,
                                        impersonate=impersonate,
                                        cd=cd, section=section)
            if val is None:
                # deferred
                return
            if ref.startswith("safe://"):
                # disallow new refs in val
                done += s[:m.start()] + str(val)
                s = s[m.end():]
            else:
                # allow new refs in val
                s = s[:m.start()] + str(val) + s[m.end():]

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
                raise ex.Error("invalid expression: %s: %s" % (expr, str(exc)))
            if m.start() == 0 and m.end() == len(s):
                # preserve the expression type
                return val
            s = s[:m.start()] + str(val) + s[m.end():]
            return s

    def handle_references(self, s, scope=False, impersonate=None, cd=None,
                          section=None):
        cacheable = self.cacheable(s)
        if cacheable:
            key = (str(s), scope, impersonate)
            if key in self.ref_cache:
                return self.ref_cache[key]
        try:
            val = self._handle_references(s, scope=scope,
                                          impersonate=impersonate,
                                          cd=cd, section=section,
                                          first_step=True)
            val = self._handle_expressions(val)
            val = self._handle_references(val, scope=scope,
                                          impersonate=impersonate,
                                          cd=cd, section=section)
        except Exception as e:
            raise
            raise ex.Error("%s: reference evaluation failed: %s" % (s, str(e)))
        if val is not None and cacheable:
            self.ref_cache[key] = val
        return val

    @staticmethod
    def cacheable(s):
        try:
            s = str(s)
        except:
            return False
        if "{rid}" in s or "{rindex}" in s:
            # those can take different values in the same service,
            # being section-dependent
            return False
        return True

    def oget_scopes(self, *args, **kwargs):
        data = {}
        for node in self.cluster_nodes:
            kwargs["impersonate"] = node
            data[node] = self.oget(*args, **kwargs)
        return data

    def oget(self, *args, **kwargs):
        """
        A wrapper around conf_get() that returns the keyword default
        instead of raising OptNotFound.
        """
        try:
            return self.conf_get(*args, **kwargs)
        except ex.OptNotFound as exc:
            return exc.default
        except ex.RequiredOptNotFound as exc:
            raise ex.Error(str(exc))

    def get_rtype(self, s, section, cd):
        if section == "DEFAULT":
            return
        try:
            return cd[s]["type"]
        except Exception as exc:
            pass
        try:
            return self.kwstore[section].getkey("type").default
        except AttributeError:
            pass

    def conf_get(self, s, o, t=None, scope=None, impersonate=None,
                 use_default=True, cd=None, verbose=True, rtype=None):
        """
        Handle keyword and section deprecation.
        """
        if cd is None:
            cd = self.cd
        section = s.split("#")[0]
        if rtype:
            pass
        elif section in self.kwstore.deprecated_sections:
            section, rtype = self.kwstore.deprecated_sections[section]
        else:
            rtype = self.get_rtype(s, section, cd)

        if rtype:
            fkey = ".".join((section, rtype, o))
        else:
            fkey = ".".join((section, o))

        deprecated_keywords = self.kwstore.reverse_deprecated_keywords.get(fkey)
        if deprecated_keywords is not None and not isinstance(deprecated_keywords, list):
            deprecated_keywords = [deprecated_keywords]

        # 1st try: supported keyword
        try:
            return self._conf_get(s, o, t=t, scope=scope,
                                  impersonate=impersonate,
                                  use_default=use_default, cd=cd,
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
        exc = False
        for deprecated_keyword in deprecated_keywords:
            try:
                return self._conf_get(s, deprecated_keyword, t=t, scope=scope,
                                      impersonate=impersonate,
                                      use_default=use_default, cd=cd,
                                      section=section, rtype=rtype)
            except ex.RequiredOptNotFound:
                exc = True
        if exc:
            self.log.error("%s.%s is mandatory" % (s, o))
            raise ex.RequiredOptNotFound

    def _conf_get(self, s, o, t=None, scope=None, impersonate=None,
                  use_default=True, cd=None, section=None, rtype=None):
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
            "cd": cd,
        }
        if s not in ("labels", "env", "data"):
            key = self.kwstore[section].getkey(o, rtype)
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
                kwargs["default_keyword"] = key.default_keyword
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
        raise ex.Error("unknown inheritance value: %s" % str(inheritance))

    def convert(self, converter, val):
        if converter == "nodes_selector":
            if hasattr(self, "path"):
                data = self.node.listener.nodes_info() if self.node.listener else None
                return self.node.nodes_selector(val, data)
            else:
                data = self.listener.nodes_info() if self.listener else None
                return self.nodes_selector(val, data)
        return globals()["convert_"+converter](val)

    def __conf_get(self, s, o, t=None, scope=None, impersonate=None,
                   use_default=None, cd=None, default=None, required=None,
                   deprecated=None, default_keyword=None):
        try:
            if not scope:
                val = self.conf_get_val_unscoped(s, o, use_default=use_default,
                                                 cd=cd,
                                                 default_keyword=default_keyword)
            else:
                val = self.conf_get_val_scoped(s, o, use_default=use_default,
                                               cd=cd,
                                               impersonate=impersonate,
                                               default_keyword=default_keyword)
        except ex.OptNotFound as exc:
            if required:
                raise ex.RequiredOptNotFound
            else:
                exc.default = copy.copy(self.handle_references(default, scope=scope,
                                                               impersonate=impersonate,
                                                               cd=cd, section=s))
                if t not in (None, "string"):
                    exc.default = self.convert(t, exc.default)
                raise exc
        try:
            val = self.handle_references(val, scope=scope,
                                         impersonate=impersonate,
                                         cd=cd, section=s)
        except ex.Error as exc:
            if o.startswith("pre_") or o.startswith("post_") or \
               o.startswith("blocking_"):
                pass
            else:
                raise

        if t in (None, "string"):
            return val
        return self.convert(t, val)

    def conf_get_val_unscoped(self, s, o, use_default=True, cd=None, default_keyword=None):
        if cd is None:
            try:
                cd = self.private_cd
            except AttributeError:
                cd = self.cd
        try:
            return cd[s][o]
        except KeyError:
            pass
        if s != "DEFAULT" and use_default and self.has_default_section:
            # fallback to default
            return self.conf_get_val_unscoped("DEFAULT", default_keyword,
                                              cd=cd)
        raise ex.OptNotFound("unscoped keyword %s.%s not found." % (s, o))

    def conf_has_option_scoped(self, s, o, nodename=None, cd=None, scope_order=None):
        """
        Handles the keyword scope_order property, at and impersonate
        """
        if s != "DEFAULT":
            try:
                options = cd[s].keys()
            except KeyError:
                return
        else:
            try:
                options = cd["DEFAULT"].keys()
            except KeyError:
                options = []

        prefix = o + "@"
        options = [option for option in options if o == option or option.startswith(prefix)]
        if not options:
            return

        candidates = [
            (o+"@"+nodename, True),
        ]
        if not hasattr(self, "path"):
            if o != "nodes":
                candidates.append((o+"@nodes", nodename in self.cluster_nodes))
            if o != "drpnodes":
                candidates.append((o+"@drpnodes", nodename in self.cluster_drpnodes))
        elif self.path == "cluster":
            if o != "nodes":
                candidates.append((o+"@nodes", nodename in self.node.cluster_nodes))
            if o != "drpnodes":
                candidates.append((o+"@drpnodes", nodename in self.node.cluster_drpnodes))
        else:
            if o != "nodes":
                candidates.append((o+"@nodes", nodename in self.nodes))
            if o != "drpnodes":
                candidates.append((o+"@drpnodes", nodename in self.drpnodes))
            if o != "encapnodes":
                candidates.append((o+"@encapnodes", nodename in self.encapnodes))
            if o != "flex_primary":
                candidates.append((o+"@flex_primary", nodename == self.flex_primary))
            if o != "drp_flex_primary":
                candidates.append((o+"@drp_flex_primary", nodename == self.drp_flex_primary))
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

    def conf_get_val_scoped(self, s, o, impersonate=None, use_default=True, cd=None, scope_order=None,
                            default_keyword=None):
        if cd is None:
            try:
                cd = self.private_cd
            except AttributeError:
                cd = self.cd
        if impersonate is None:
            nodename = Env.nodename
        else:
            nodename = impersonate

        option = self.conf_has_option_scoped(s, o, nodename=nodename,
                                             cd=cd,
                                             scope_order=scope_order)
        if option is None:
            if not self.has_default_section or not use_default:
                raise ex.OptNotFound("scoped keyword %s.%s not found." % (s, o))

            if use_default and self.has_default_section:
                if s != "DEFAULT":
                    # fallback to default
                    return self.conf_get_val_scoped("DEFAULT", default_keyword,
                                                    impersonate=impersonate,
                                                    cd=cd,
                                                    scope_order=scope_order)
                else:
                    raise ex.OptNotFound("scoped keyword %s.%s not found." % (s, o))

        try:
            val = cd[s][option]
        except KeyError:
            raise ex.Error("param %s.%s is not set" % (s, o))

        return val

    def validate_config(self, cd=None, path=None):
        """
        The validate config action entrypoint.
        """
        ret = self._validate_config(cd=cd, path=path)
        return ret["warnings"] + ret["errors"]

    def _validate_config(self, cd=None, path=None):
        """
        The validate config core method.
        Returns a dict with the list of syntax warnings and errors.
        """
        if path:
            cd = self.parse_config_file(path)
        elif not cd:
            try:
                cd = self.private_cd
            except AttributeError:
                cd = self.cd

        ret = {
            "errors": 0,
            "warnings": 0,
        }

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
            value = cd.get(section, {}).get(option)
            if not is_string(value) \
                    or ".exposed_devs" in value \
                    or ".base_devs" in value \
                    or ".sub_devs" in value \
                    or re.match(r"volume#.*\.mnt", value):
                return 0
            try:
                deref = self.handle_references(value, scope=True, cd=cd,
                                               section=section)
            except ex.Error as exc:
                if not option.startswith("pre_") and \
                   not option.startswith("post_") and \
                   not option.startswith("blocking_"):
                    self.log.error(str(exc))
                    return 1
            except Exception as exc:
                self.log.error(str(exc))
                return 1
            if deref is None:
                self.log.warning("broken reference: %s.%s", section, option)
            return 0

        def get_val(key, section, option, verbose=True, impersonate=None):
            """
            Fetch the value and convert it to the expected type.
            """
            _option = option.split("@")[0]
            value = self.conf_get(section, _option, cd=cd, verbose=verbose, impersonate=impersonate)
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
            impersonate = None
            if hasattr(self, "encap"):
                if self.encap and section not in self.resources_by_id:
                    # encap node does not validate global resource values
                    return err
                elif not self.encap and section in self.encap_resources:
                    impersonate = list(self.encapnodes)[0]
            try:
                value = get_val(key, section, option, verbose=False, impersonate=impersonate)
            except ValueError as exc:
                self.log.warning(str(exc))
                return 0
            except ex.OptNotFound:
                return 0
            except ex.RequiredOptNotFound:
                if hasattr(self, "path"):
                    # no need to err here: already caught by svc build
                    return err
                else:
                    self.log.error("%s.%s is mandatory" % (section, option))
                    err += 1
                    return err
            err += check_candidates(key, section, option, value)
            return err

        def validate_default_options(ret):
            """
            Validate DEFAULT section options.
            """
            if not self.has_default_section:
                return ret
            for option in cd.get("DEFAULT", {}):
                if option == "comment":
                    continue
                key = self.kwstore.sections["DEFAULT"].getkey(option)
                if key is None:
                    found = False
                    # the option can be set in the DEFAULT section for the
                    # benefit of a resource section
                    for section in cd:
                        family = section.split("#")[0]
                        rtype = self.get_rtype(section, family, cd)
                        if family not in list(self.kwstore.sections.keys()) + \
                           list(self.kwstore.deprecated_sections.keys()):
                            continue
                        if family in self.kwstore.deprecated_sections:
                            results = self.kwstore.deprecated_sections[family]
                            family = results[0]
                        if self.kwstore.sections[family].getkey(option, rtype) is not None:
                            found = True
                            break
                    if not found:
                        self.log.warning("ignored option DEFAULT.%s", option)
                        ret["warnings"] += 1
                else:
                    # here we know its a native DEFAULT option
                    ret["errors"] += check_known_option(key, "DEFAULT", option)
            return ret

        def validate_resources_options(ret):
            """
            Validate resource sections options.
            """
            for section in cd:
                if section in ("labels", "env", "data"):
                    # the "env" section is not handled by a resource driver, and is
                    # unknown to the kwstore. Just ignore it.
                    continue
                family = section.split("#")[0]
                rtype = self.get_rtype(section, family, cd)
                if family not in ("DEFAULT", "labels", "env", "data", "subset"):
                    try:
                        loader = self.load_driver
                    except AttributeError:
                        pass
                    else:
                        try:
                            loader(family, rtype)
                        except Exception:
                            pass
                if family not in list(self.kwstore.sections.keys()) + list(self.kwstore.deprecated_sections.keys()):
                    self.log.warning("ignored section %s", section)
                    ret["warnings"] += 1
                    continue
                if family in self.kwstore.deprecated_sections:
                    self.log.warning("deprecated section prefix %s", family)
                    ret["warnings"] += 1
                    family, rtype = self.kwstore.deprecated_sections[family]
                for option in cd.get(section, {}):
                    if option == "comment":
                        continue
                    if option in cd.get("DEFAULT", {}):
                        continue
                    key = self.kwstore.sections[family].getkey(option, rtype=rtype)
                    if key is None:
                        key = self.kwstore.sections[family].getkey(option)
                    if key is None:
                        self.log.warning("ignored option %s.%s%s", section,
                                         option, ", driver %s" % rtype if rtype else "")
                        ret["warnings"] += 1
                    else:
                        ret["errors"] += check_known_option(key, section, option)
            return ret

        def validate_build(ret):
            """
            Try a service build to catch errors missed in other tests.
            """
            if not hasattr(self, "path"):
                return ret
            svc = None
            try:
                svc = factory(self.kind)(self.name, namespace=self.namespace,
                                         cd=cd, node=self.node, volatile=True)
            except Exception as exc:
                self.log.error("the new configuration causes the following "
                               "build error: %s", str(exc))
                ret["errors"] += 1
            if svc:
                try:
                    ret["errors"] += svc.init_resources()
                except Exception as exc:
                    self.log.error(exc)
                    ret["errors"] += 1
            return ret

        ret = validate_build(ret)
        ret = validate_default_options(ret)
        ret = validate_resources_options(ret)

        return ret

    def _read_cf(self):
        """
        Return the service config file content.
        """
        if not os.path.exists(self.paths.cf):
            return ""
        with codecs.open(self.paths.cf, "r", "utf8") as ofile:
            buff = ofile.read()
        return buff

    def skip_config_section(self, section):
        return False

    def print_config_data(self, src_config=None, evaluate=False, impersonate=None):
        """
        Return a simple dict (OrderedDict if possible), fed with the
        service configuration sections and keys
        """
        if src_config:
            data = self.parse_config_file(src_config)
        else:
            try:
                cd = self.private_cd
            except AttributeError:
                cd = self.cd
            data = type(cd)(cd)
        meta = {}
        if hasattr(self, "namespace"):
            meta.update({
                "name": self.name,
                "kind": self.kind,
                "namespace": self.namespace,
            })
        else:
            meta.update({
                "kind": "node",
            })

        if not evaluate:
            data["metadata"] = meta
            return data
        edata = {}
        for section, _data in data.items():
            if self.skip_config_section(section):
                continue
            edata[section] = {}
            keys = []
            for key in _data:
                key = key.split("@")[0]
                if key in edata[section]:
                    continue
                try:
                    val = self.conf_get(section, key, impersonate=impersonate)
                except (ex.RequiredOptNotFound, ex.OptNotFound):
                    continue
                except ValueError:
                    raise
                # ensure the data is json-exportable
                if isinstance(val, set):
                    val = list(val)
                edata[section][key] = val
        edata["metadata"] = meta
        return edata

    @lazy
    def labels(self):
        try:
            cd = self.private_cd
        except AttributeError:
            cd = self.cd
        data = {}
        try:
            for label, value in cd.get("labels", {}).items():
                try:
                    cd["DEFAULT"][label]
                    continue
                except KeyError:
                    pass
                data[label] = value
        except Exception:
            pass
        return data

    def section_kwargs(self, section, rtype=None):
        kwargs = {}
        try:
            cat = section.split("#")[0]
        except ValueError:
            return kwargs
        for keyword in self.kwstore.all_keys(cat, rtype):
            try:
                kwargs[keyword.protoname] = self.conf_get(section, keyword.keyword, rtype=rtype, verbose=False)
            except ex.RequiredOptNotFound:
                try:
                    if keyword.provisioning and (self.running_action != "provision" or self.oget(section, "provision") == False):
                        continue
                except AttributeError:
                    # not a BaseSvc
                    pass
                self.log.error("%s.%s is mandatory" % (section, keyword.keyword))
                raise
            except ex.OptNotFound as exc:
                kwargs[keyword.protoname] = exc.default
        return kwargs

    def conf_sections(self, cat=None, cd=None):
        if cd is None:
            cd = self.cd
        for section in cd:
            if cat is None or section.startswith(cat+"#"):
                yield section

    def parse_config_file(self, cf=None):
        self.clear_ref_cache()
        if cf is None:
            cf = self.paths.cf
        try:
            config = read_cf(cf)
        except Exception as exc:
            import traceback
            traceback.print_stack()
            raise ex.Error("error parsing %s: %s" % (cf, exc))
        try:
            from collections import OrderedDict
            best_dict = OrderedDict
        except ImportError:
            best_dict = dict
        data = best_dict()
        tmp = best_dict()
        defaults = config.defaults()
        for key in defaults.keys():
            tmp[key] = defaults[key]

        if tmp:
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

        comments = read_cf_comments(cf)
        for section, comments in comments.items():
            if section in data:
                if "comment" not in data[section]:
                    data[section]["comment"] = ""
                else:
                    data[section]["comment"] += "\n"
                data[section]["comment"] += "\n".join(comments)
            else:
                if "DEFAULT" not in data:
                    data["DEFAULT"] = {}
                if "comment" not in data["DEFAULT"]:
                    data["DEFAULT"]["comment"] = ""
                else:
                    data["DEFAULT"]["comment"] += "\n"
                data["DEFAULT"]["comment"] += "\n".join(comments)

        return data

    def is_volatile(self):
        try:
            if self.volatile:
                return True
        except AttributeError:
            pass
        return False

    def commit(self, cd=None, cf=None, validation=True):
        """
        Installs a service configuration file from section, keys and values
        fed from a data structure.
        """
        if cd is None:
            try:
                cd = self.private_cd
            except AttributeError:
                cd = self.cd
        if cf is None:
            cf = self.paths.cf
        if not isinstance(cd, dict):
            return
        if "metadata" in cd:
            del cd["metadata"]
        if hasattr(self, "new_id") and "id" not in cd.get("DEFAULT", {}):
            if "DEFAULT" not in cd:
                cd["DEFAULT"] = {}
            if self.is_volatile():
                cd["DEFAULT"]["id"] = self.new_id()
            else:
                current_id = self.parse_config_file(cf).get("DEFAULT", {}).get("id")
                if current_id:
                    cd["DEFAULT"]["id"] = current_id
                else:
                    cd["DEFAULT"]["id"] = self.new_id()
        if validation:
            ret = self._validate_config()
            if ret["errors"]:
                raise ex.Error

        if not self.is_volatile():
            self.dump_config_data(cd=cd, cf=cf)

        self.clear_ref_cache()
        self.post_commit()

    def post_commit(self):
        """
        Place holder for things to do on the child class instance after a commit.
        """
        pass

    def dump_config_data(self, cd=None, cf=None):
        import tempfile
        import shutil
        if cf is None:
            cf = self.paths.cf
        dirpath = os.path.dirname(cf)
        makedirs(dirpath)
        tmpf = tempfile.NamedTemporaryFile(delete=False, dir=dirpath, prefix=os.path.basename(cf)+".")
        tmpfpath = tmpf.name
        tmpf.close()
        os.chmod(tmpfpath, 0o0600)
        lines = []

        for section_name, section in cd.items():
            lines.append("[%s]" % section_name)
            for key, value in section.items():
                if value is None:
                    continue
                if key != "comment":
                    lines.append("%s = %s" % (key, str(value).replace("\n", "\n\t")))
                else:
                    lines += map(lambda x: "# "+x if x else "", value.split("\n"))
            lines.append("")

        try:
            buff = "\n".join(lines)
            if six.PY2:
                with codecs.open(tmpfpath, "w", "utf-8") as ofile:
                    ofile.write(buff)
            else:
                with open(tmpfpath, "w") as ofile:
                    ofile.write(buff)
            shutil.move(tmpfpath, cf)
        except Exception as exc:
            raise ex.Error("failed to write %s: %s" % (cf, exc))
        finally:
            try:
                os.unlink(tmpfpath)
            except Exception:
                pass
