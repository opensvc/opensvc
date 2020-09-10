"""
The module implementing Keyword, Section and KeywordStore classes,
used to declared node and service configuration keywords and their
properties.
"""
from __future__ import print_function
import os
import copy
from textwrap import TextWrapper

import core.exceptions as ex
from env import Env

class MissKeyNoDefault(Exception):
    pass

class KeyInvalidValue(Exception):
    pass

class Keyword(object):
    def __init__(self, section, keyword,
                 rtype=None,
                 protoname=None,
                 required=False,
                 generic=False,
                 at=False,
                 inheritance="leaf > head",
                 scope_order="specific > generic",
                 default=None,
                 default_text=None,
                 default_keyword=None,
                 candidates=None,
                 strict_candidates=True,
                 convert=None,
                 depends=None,
                 text="",
                 example=None,
                 provisioning=False):
        if depends is None:
            depends = []
        self.protoname = protoname or keyword
        self.section = section
        self.keyword = keyword
        self.default_keyword = default_keyword or keyword
        if rtype is None or isinstance(rtype, list):
            self.rtype = rtype
        else:
            self.rtype = [rtype]
        self.generic = generic
        self.at = at
        self.top = None
        self.required = required
        self.default = default
        self.default_text = default_text
        self.candidates = candidates
        self.strict_candidates = strict_candidates
        self.depends = depends
        self.text = text
        self.provisioning = provisioning
        self.convert = convert
        self.inheritance = inheritance
        self.scope_order = scope_order
        if example is not None:
            self.example = example
        elif self.convert == "size":
            self.example = "100m"
        elif self.convert == "duration":
            self.example = "1h"
        elif self.convert in ("boolean", "tristate"):
            self.example = "false"
        else:
            self.example = "foo"

        if self.default_text is None:
            self.default_text = self.default

    def __repr__(self):
        return "<Keyword %s>" % self.keyword

    def __str__(self):
        return "<Keyword %s>" % self.keyword

    def __lt__(self, o):
        return self.section + self.keyword < o.section + o.keyword

    def __getattribute__(self, attr):
        if attr == "default":
            return copy.copy(object.__getattribute__(self, attr))
        return object.__getattribute__(self, attr)

    def deprecated(self):
        if self.keyword in self.top.deprecated_keywords:
            return True
        if self.rtype is None:
            return self.section+"."+self.keyword in self.top.deprecated_keywords
        for rtype in self.rtype:
            if rtype is None:
                if self.section+"."+self.keyword in self.top.deprecated_keywords:
                    return True
            elif self.section+"."+rtype+"."+self.keyword in self.top.deprecated_keywords:
                return True
        return False

    def template(self, fmt="text", section=None):
        if self.deprecated():
            return ''

        if fmt == "text":
            return self.template_text()
        elif fmt == "rst":
            return self.template_rst(section=section)
        else:
            return ""

    def template_text(self):
        wrapper = TextWrapper(subsequent_indent="#%18s"%"", width=78)

        depends = " && ".join(["%s in %s"%(d[0], d[1]) for d in self.depends])
        if depends == "":
            depends = None

        if isinstance(self.candidates, (list, tuple, set)):
            candidates = " | ".join([str(x) for x in self.candidates])
        else:
            candidates = str(self.candidates)
        if not self.strict_candidates:
            candidates += " ..."

        s = '#\n'
        s += "# keyword:          %s\n"%self.keyword
        s += "# ----------------------------------------------------------------------------\n"
        s += "#  scopable:        %s\n"%str(self.at)
        s += "#  required:        %s\n"%str(self.required)
        if self.top.has_default_section:
            s += "#  provisioning:    %s\n"%str(self.provisioning)
        s += "#  default:         %s\n"%str(self.default_text)
        if self.top.has_default_section:
            s += "#  inheritance:     %s\n"%str(self.inheritance)
            if self.keyword != self.default_keyword:
                s += "#  default keyword: %s\n" % self.default_keyword
        s += "#  scope order:     %s\n"%str(self.scope_order)
        if self.candidates:
            s += "#  candidates:      %s\n"%candidates
        if depends:
            s += "#  depends:         %s\n"%depends
        if self.convert:
            s += "#  convert:         %s\n"%str(self.convert)
        s += '#\n'
        if self.text:
            wrapper = TextWrapper(subsequent_indent="#%9s"%"", width=78)
            s += wrapper.fill("#  desc:  "+self.text) + "\n"
        s += '#\n'
        if self.default_text is not None:
            val = self.default_text
        elif self.candidates and len(self.candidates) > 0:
            val = self.candidates[0]
        else:
            val = self.example
        s += ";" + self.keyword + " = " + str(val) + "\n\n"
        return s

    def template_rst(self, section=None):
        depends = " && ".join(["%s in %s"%(d[0], d[1]) for d in self.depends])
        if depends == "":
            depends = None

        if isinstance(self.candidates, (list, tuple, set)):
            candidates = " | ".join([str(x) for x in self.candidates])
        else:
            candidates = str(self.candidates)
        if not self.strict_candidates:
            candidates += " ..."

        s = ""
        if section:
            fill=""
            if "template.node" in self.top.template_prefix:
                fill="node."
            if "template.cluster" in self.top.template_prefix:
                fill="cluster."
            if "template.secret" in self.top.template_prefix:
                fill="secret."
            if "template.cfg" in self.top.template_prefix:
                fill="cfg."
            s += ".. _%s%s.%s:\n\n" % (fill, section, self.keyword)

        s += ':kw:`%s`\n' % self.keyword
        s += "=" * (len(self.keyword) + 6) + "\n"
        s += "\n"
        s += "================= ================================================================\n"
        s += "**scopable**      %s\n"%str(self.at)
        s += "**required**      %s\n"%str(self.required)
        if self.top.has_default_section:
            s += "**provisioning**  %s\n"%str(self.provisioning)
        s += "**default**       %s\n"%str(self.default_text)
        if self.top.has_default_section:
            s += "**inheritance**   %s\n"%str(self.inheritance)
            if self.keyword != self.default_keyword:
                s += "**default keyword** %s\n" % self.default_keyword
        s += "**scope order**   %s\n"%str(self.scope_order)
        if self.candidates:
            s += "**candidates**    %s\n"%candidates
        if depends:
            s += "**depends**       %s\n"%depends
        if self.convert:
            s += "**convert**       %s\n"%str(self.convert)
        s += "================= ================================================================\n"
        s += '\n'
        if self.text:
            s += self.text + "\n"
        s += '\n'
        return s

    def dump(self):
        data = {"keyword": self.keyword}
        if self.rtype:
            data["type"] = self.rtype
        if self.at:
            data["at"] = self.at
        if self.required:
            data["required"] = self.required
        if self.candidates:
            data["candidates"] = self.candidates
            data["strict_candidates"] = self.strict_candidates
        if self.default:
            data["default"] = self.default
        if self.default_text:
            data["default_text"] = self.default_text
        if self.default_keyword != self.keyword:
            data["default_keyword"] = self.default_keyword
        if self.inheritance:
            data["inheritance"] = self.inheritance
        if self.scope_order:
            data["scope_order"] = self.scope_order
        if self.provisioning:
            data["provisioning"] = self.provisioning
        if self.depends:
            data["depends"] = self.depends
        if self.convert:
            data["convert"] = self.convert
        else:
            data["convert"] = "string"
        if self.text:
            data["text"] = self.text
        return data

class Section(object):
    def __init__(self, section, top=None):
        self.section = section
        self.top = top
        self.data = {}
        self.sigs = []
        self.rtypes = set()

    def __repr__(self):
        return "<Section %s keywords:%d>" % (self.section, len(self.data))

    def __str__(self):
        return "<Section %s keywords:%d>" % (self.section, len(self.data))

    def __iadd__(self, o):
        if not isinstance(o, Keyword):
            return self
        if o.rtype is None:
            sig = (None, o.keyword)
            if sig in self.data:
                return
            self.data[sig] = o
        else:
            for rt in o.rtype:
                sig = (rt, o.keyword)
                if sig in self.data:
                    continue
                self.data[sig] = o
                if rt is not None:
                    self.rtypes.add(rt)
        return self

    @property
    def keywords(self):
        return self.data.values()

    def dump(self):
        data = []
        for kw in self.keywords:
            data.append(kw.dump())
        return data

    def template(self, fmt="text", write=False):
        k = self.getkey("type")
        if k is None:
            return self._template(fmt=fmt, write=write)
        if k.candidates is None:
            return self._template(fmt=fmt, write=write)
        s = ""
        if not k.strict_candidates:
            s += self._template(fmt=fmt, write=write)
        for t in k.candidates:
            s += self._template(t, fmt=fmt, write=write)
        return s

    def _template(self, rtype=None, fmt="text", write=False):
        section = self.section
        if self.section in self.top.deprecated_sections:
            return ""
        if rtype and self.top and self.section+"."+rtype in self.top.deprecated_sections:
            return ""
        if fmt == "text":
            return self._template_text(rtype, section, write=write)
        elif fmt == "rst":
            return self._template_rst(rtype, section, write=write)
        else:
            return ""

    def _template_text(self, rtype, section, write=False):
        fpath = os.path.join(Env.paths.pathdoc, self.top.template_prefix+section+".conf")
        if rtype:
            section += ", type "+rtype
            fpath = os.path.join(Env.paths.pathdoc, self.top.template_prefix+self.section+"."+rtype+".conf")
        s = "#"*78 + "\n"
        s += "# %-74s #\n" % " "
        s += "# %-74s #\n" % section
        s += "# %-74s #\n" % " "
        s += "#"*78 + "\n\n"
        if section in self.top.base_sections:
            s += "[%s]\n" % self.section
        else:
            s += "[%s#rindex]\n" % self.section
        done = []
        if rtype is not None:
            s += ";type = " + rtype + "\n\n"
            for keyword in sorted(self.getkeys(rtype)):
                s += keyword.template(fmt="text")
                done.append(keyword.keyword)
        for keyword in sorted(self.getprovkeys(rtype)):
            if keyword.keyword in done:
                continue
            s += keyword.template(fmt="text")
        for keyword in sorted(self.getkeys()):
            if keyword.keyword in done:
                continue
            if keyword.keyword == "type":
                continue
            s += keyword.template(fmt="text")
        if write:
            print("write", fpath)
            with open(fpath, "w") as f:
                f.write(s)
        return s

    def _template_rst(self, rtype, section, write=False):
        dpath = os.path.join(Env.paths.pathtmp, "rst")
        if not os.path.exists(dpath):
            os.makedirs(dpath)
        if rtype:
            section += "."+rtype
            fpath = os.path.join(dpath, self.top.template_prefix+self.section+"."+rtype+".rst")
        else:
            fpath = os.path.join(dpath, self.top.template_prefix+section+".rst")
        s = section + "\n"
        s += "*" * len(section) + "\n\n"
        if self.top.template_prefix != "template.node." and self.top.template_prefix != "template.cluster." and len(section.split('.')) > 1:
            s += ".. include:: template.service." + section + ".example\n\n"
        for keyword in sorted(self.getkeys(rtype)):
            s += keyword.template(fmt="rst", section=section)
        for keyword in sorted(self.getprovkeys(rtype)):
            s += keyword.template(fmt="rst", section=section)
        if rtype is not None:
            for keyword in sorted(self.getkeys()):
                if keyword.keyword == "type":
                    continue
                s += keyword.template(fmt="rst", section=section)
        if write:
            print("write", fpath)
            with open(fpath, "w") as f:
                f.write(s)
        return s

    def getallkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None]
        elif rtype in self.rtypes:
            return [k for k in self.keywords if k.rtype and rtype in k.rtype]
        else:
            # non-strict rtype candidates. ex: fs.vfat falls back to fs
            return [k for k in self.keywords if k.rtype == ""]

    def getkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None and not k.provisioning]
        elif rtype in self.rtypes:
            return [k for k in self.keywords if k.rtype and rtype in k.rtype and not k.provisioning]
        else:
            return [k for k in self.keywords if k.rtype == "" and not k.provisioning]

    def getprovkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None and k.provisioning]
        elif rtype in self.rtypes:
            return [k for k in self.keywords if k.rtype and rtype in k.rtype and k.provisioning]
        else:
            return [k for k in self.keywords if k.rtype == "" and k.provisioning]

    def getkey(self, keyword, rtype=None):
        try:
            keyword, _ = keyword.split("@", 1)
        except ValueError:
            pass
        k = None
        if rtype:
            if rtype not in self.rtypes:
                # unknown rtype, allowed by non-strict candidates,
                # are routed to special type ''.
                rtype = ""
                fkey = ".".join((self.section, keyword))
            else:
                fkey = ".".join((self.section, rtype, keyword))
            if fkey in self.top.deprecated_keywords:
                keyword = self.top.deprecated_keywords[fkey]
                if keyword is None:
                    return
            k = self.data.get((rtype, keyword))
            if not k:
                k = self.data.get((None, keyword))
        else:
            fkey = ".".join((self.section, keyword))
            if fkey in self.top.deprecated_keywords:
                keyword = self.top.deprecated_keywords[fkey]
            k = self.data.get((None, keyword))
        return k

class KeywordStore(dict):
    def __init__(self, name=None, provision=False, keywords=None, deprecated_keywords=None,
                 reverse_deprecated_keywords=None,
                 deprecated_sections=None, template_prefix="template.",
                 base_sections=None, has_default_section=True):
        dict.__init__(self)
        self.name = name
        self.sections = {}
        self.deprecated_sections = deprecated_sections or {}
        self.deprecated_keywords = deprecated_keywords or {}
        self.reverse_deprecated_keywords = reverse_deprecated_keywords or {}
        self.template_prefix = template_prefix
        self.base_sections = base_sections or ["DEFAULT"]
        self.provision = provision
        self.has_default_section = has_default_section
        self.modules = set()

        for keyword in keywords or []:
            sections = keyword.get("sections", [keyword.get("section")])
            prefixes = keyword.get("prefixes", [""])
            for section in sections:
                for prefix in prefixes:
                    data = dict((key, val) for (key, val) in keyword.items() if key not in ("sections", "prefixes"))
                    try:
                        data.update({
                            "section": section,
                            "keyword": prefix+keyword["keyword"],
                            "text": keyword["text"].replace("{prefix}", prefix),
                        })
                        self += Keyword(**data)
                    except KeyError as exc:
                        raise ex.Error("misformatted keyword definition: %s: %s" % (exc, data))

    def __str__(self):
        return "<KeywordStore name:%s sections:%d keywords:%d>" % (self.name, len(self.sections), self.keywords_count())

    def keywords_count(self):
        n = 0
        for section in self.sections.values():
            n += len(section.keywords)
        return n

    def register_driver(self, driver_group, driver_basename, keywords=None, driver_basename_aliases=None, **kwargs):
        keywords = [
            dict(k, section=driver_group, rtype=driver_basename) for k in keywords
        ]
        self += KeywordStore(
            keywords=keywords,
            **kwargs
        )
        driver_basename_aliases = driver_basename_aliases or []
        for alias in driver_basename_aliases:
            keywords = [
                dict(k, section=driver_group, rtype=alias) for k in keywords
            ]
            self += KeywordStore(
                keywords=keywords,
                **kwargs
            )

    def driver_kwstore(self, mod=None, modname=None):
        if mod is None:
            try:
                mod = __import__(modname)
            except Exception:
                return
        if modname is None:
            modname = mod.__name__
        if modname in self.modules:
            # already merged
            return
        self.modules.add(modname)
        kwargs = {
            "name": modname,
            "provision": True,
            "base_sections": ["env", "DEFAULT"],
            "template_prefix": "template.service.",
        }
        # mandatory attributes:
        # - DRIVER_GROUP
        # - DRIVER_BASENAME
        # - KEYWORDS
        try:
            kwargs["keywords"] = [
                dict(k, section=mod.DRIVER_GROUP, rtype=mod.DRIVER_BASENAME)
                for k in getattr(mod, "KEYWORDS")
            ]
        except AttributeError:
            return

        # optional attributes:
        # - DEPRECATED_SECTIONS
        # - DEPRECATED_KEYWORDS
        # - REVERSE_DEPRECATED_KEYWORDS
        # - DRIVER_BASENAME_ALIASES
        for kwarg in ("deprecated_sections", "deprecated_keywords", "reverse_deprecated_keywords"):
            try:
                kwargs[kwarg] = getattr(mod, kwarg.upper())
            except:
                pass
        try:
            aliases = getattr(mod, "DRIVER_BASENAME_ALIASES")
        except AttributeError:
            aliases = []
        for alias in aliases:
            kwargs["keywords"] += [
                dict(k, section=mod.DRIVER_GROUP, rtype=alias)
                for k in getattr(mod, "KEYWORDS")
            ]

        kwstore = KeywordStore(**kwargs)
        return kwstore

    def __iadd__(self, o):
        if o is None:
            return self
        if isinstance(o, Keyword):
            return self.__iadd_keyword__(o)
        if isinstance(o, Section):
            return self.__iadd_section__(o)
        if isinstance(o, KeywordStore):
            return self.__iadd_keyword_store__(o)
        if isinstance(o, str):
            return self.__iadd_drivername__(o)
        if hasattr(o, "KEYWORDS"):
            return self.__iadd_driver__(o)
        return self

    def __iadd_driver__(self, other):
        self += self.driver_kwstore(mod=other)
        return self

    def __iadd_drivername__(self, other):
        self += self.driver_kwstore(modname=other)
        return self

    def __iadd_keyword_store__(self, other):
        for section in other.sections.values():
            self += section
        self.deprecated_sections.update(other.deprecated_sections)
        self.deprecated_keywords.update(other.deprecated_keywords)
        self.reverse_deprecated_keywords.update(other.reverse_deprecated_keywords)
        return self

    def __iadd_section__(self, section):
        #print("   ", section.section)
        if section.section not in self.sections:
            self.sections[section.section] = Section(section.section, top=self)
        rtype_key = self[section.section].getkey("type")
        for kw in section.keywords:
            #print("    ", kw.keyword)
            self.sections[section.section] += kw
            if rtype_key \
               and isinstance(rtype_key.candidates, list):
                rtypes = kw.rtype if kw.rtype is not None else [None]
                for rtype in rtypes:
                    if rtype not in rtype_key.candidates:
                        rtype_key.candidates.append(rtype)
        return self

    def __iadd_keyword__(self, o):
        o.top = self
        if o.section not in self.sections:
            self.sections[o.section] = Section(o.section, top=self)
        self.sections[o.section] += o
        return self

    def __getattr__(self, key):
        return self.sections[str(key)]

    def __getitem__(self, key):
        k = str(key)
        if k not in self.sections:
            return Section(k, top=self)
        return self.sections[k]

    def dump(self):
        data = {}
        for section in sorted(self.sections):
            data[section] = self.sections[section].dump()
        return data

    def print_templates(self, fmt="text"):
        """
        Print templates in the spectified format (text by default, or rst).
        """
        for section in sorted(self.sections):
            print(self.sections[section].template(fmt=fmt))

    def write_templates(self, fmt="text"):
        """
        Write templates in the spectified format (text by default, or rst).
        """
        for section in sorted(self.sections):
            self.sections[section].template(fmt=fmt, write=True)

    def required_keys(self, section, rtype=None):
        """
        Return the list of required keywords in the section for the resource
        type specified by <rtype>.
        """
        try:
            return [k for k in sorted(self.sections[section].getkeys(rtype)) if k.required is True]
        except KeyError:
            return []

    def optional_keys(self, section, rtype=None):
        """
        Return the list of optional keywords in the section for the resource
        type specified by <rtype>.
        """
        try:
            return [k for k in sorted(self.sections[section].getkeys(rtype)) if k.required is False]
        except KeyError:
            return []

    def all_keys(self, section, rtype=None):
        """
        Return the list of optional keywords in the section for the resource
        type specified by <rtype>.
        """
        try:
            return sorted(self.sections[section].getallkeys(rtype))
        except KeyError:
            return []

    def section_kwargs(self, cat, rtype=None):
        kwargs = {}
        for keyword in self.all_keys(cat, rtype):
            try:
                kwargs[keyword.name] = self.conf_get(cat, keyword.name)
            except ex.RequiredOptNotFound:
                raise
            except ex.OptNotFound as exc:
                kwargs[keyword.name] = exc.default
        return kwargs

    def purge_keywords_from_dict(self, d, section):
        """
        Remove unknown keywords from a section.
        """
        if section == "env":
            return d
        if 'type' in d:
            rtype = d['type']
        else:
            rtype = None
        delete_keywords = []
        for keyword in d:
            key = self.sections[section].getkey(keyword)
            if key is None and rtype is not None:
                key = self.sections[section].getkey(keyword, rtype)
            if key is None:
                if keyword != "rtype":
                    print("Remove unknown keyword '%s' from section '%s'"%(keyword, section))
                    delete_keywords.append(keyword)

        for keyword in delete_keywords:
            del d[keyword]

        return d

    def update(self, rid, d):
        """
        Given a resource dictionary, spot missing required keys
        and provide a new dictionary to merge populated by default
        values.
        """
        import copy
        completion = copy.copy(d)

        # decompose rid into section and rtype
        if rid in ('DEFAULT', 'env'):
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
            if section == "env":
                break
            if section not in self.sections:
                raise KeyInvalidValue("'%s' driver family is not valid in section '%s'"%(section, rid))
            key = self.sections[section].getkey(keyword)
            if key is None and rtype is not None:
                key = self.sections[section].getkey(keyword, rtype)
            if key is None:
                continue
            if key.strict_candidates and key.candidates is not None and value not in key.candidates:
                raise KeyInvalidValue("'%s' keyword has invalid value '%s' in section '%s'"%(keyword, str(value), rid))

        # add missing required keys if they have a known default value
        for key in self.required_keys(section, rtype):
            fkey = ".".join((section, str(rtype), key.keyword))
            if fkey in self.deprecated_keywords:
                continue

            if key.keyword in d:
                continue
            if key.keyword in [x.split('@')[0] for x in d]:
                continue
            if key.default is None:
                raise MissKeyNoDefault("No default value for required key '%s' in section '%s'"%(key.keyword, rid))
            print("Implicitely add [%s] %s = %s" % (rid, key.keyword, str(key.default)))
            completion[key.keyword] = key.default

        # purge unknown keywords and provisioning keywords
        completion = self.purge_keywords_from_dict(completion, section)

        return completion
