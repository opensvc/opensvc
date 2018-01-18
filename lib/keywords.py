import sys
import os
from rcGlobalEnv import rcEnv
from textwrap import TextWrapper

if sys.version_info[0] >= 3:
    raw_input = input

class MissKeyNoDefault(Exception):
     pass

class KeyInvalidValue(Exception):
     pass

class Keyword(object):
    def __init__(self, section, keyword,
                 rtype=None,
                 order=100,
                 required=False,
                 generic=False,
                 at=False,
                 inheritance="leaf > head",
                 scope_order="specific > generic",
                 default=None,
                 default_text=None,
                 candidates=None,
                 strict_candidates=True,
                 convert=None,
                 depends=[],
                 text="",
                 example="foo",
                 provisioning=False):
        self.section = section
        self.keyword = keyword
        if rtype is None or type(rtype) == list:
            self.rtype = rtype
        else:
            self.rtype = [rtype]
        self.order = order
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
        self.example = example
        self.provisioning = provisioning
        self.convert = convert
        self.inheritance = inheritance
        self.scope_order = scope_order

        if self.default_text is None:
            self.default_text = self.default

    def __lt__(self, o):
        return self.order < o.order

    def deprecated(self):
        if self.keyword in self.top.deprecated_keywords:
            return True
        if self.rtype is None:
            if self.section+"."+self.keyword in self.top.deprecated_keywords:
                return True
            else:
                return False
        for rtype in self.rtype:
            if self.section+"."+rtype+"."+self.keyword in self.top.deprecated_keywords:
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

        depends = " && ".join(map(lambda d: "%s in %s"%(d[0], d[1]), self.depends))
        if depends == "":
            depends = None

        if type(self.candidates) in (list, tuple, set):
            candidates = " | ".join(map(lambda x: str(x), self.candidates))
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
        depends = " && ".join(map(lambda d: "%s in %s"%(d[0], d[1]), self.depends))
        if depends == "":
            depends = None

        if type(self.candidates) in (list, tuple, set):
            candidates = " | ".join(map(lambda x: str(x), self.candidates))
        else:
            candidates = str(self.candidates)
        if not self.strict_candidates:
            candidates += " ..."

        s = ""
        if section:
            s += ".. _%s.%s:\n\n" % (section, self.keyword)

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

    def __str__(self):
        if self.deprecated():
            return ''

        wrapper = TextWrapper(subsequent_indent="%16s"%"", width=78)

        depends = ""
        for d in self.depends:
            depends += "%s in %s\n"%(d[0], d[1])
        if depends == "":
            depends = None

        if type(self.candidates) in (list, tuple, set):
            candidates = " | ".join(map(lambda x: str(x), self.candidates))
        else:
            candidates = str(self.candidates)
        if not self.strict_candidates:
            candidates += " ..."

        s = ''
        s += "------------------------------------------------------------------------------\n"
        s += "section:        %s\n"%self.section
        s += "keyword:        %s\n"%self.keyword
        s += "------------------------------------------------------------------------------\n"
        s += "  required:     %s\n"%str(self.required)
        s += "  provisioning: %s\n"%str(self.provisioning)
        s += "  default:      %s\n"%str(self.default)
        s += "  candidates:   %s\n"%candidates
        s += "  depends:      %s\n"%depends
        s += "  scopable:     %s\n"%str(self.at)
        if self.text:
            s += wrapper.fill("  help:         "+self.text)
        if self.at:
            s += "\n\nPrefix the value with '@<node> ', '@nodes ', '@drpnodes ', '@flex_primary', '@drp_flex_primary' or '@encapnodes '\n"
            s += "to specify a scope-specific value.\n"
            s += "You will be prompted for new values until you submit an empty value.\n"
        s += "\n"
        return s

    def form(self, d):
        if self.deprecated():
            return

        # skip this form if dependencies are not met
        for d_keyword, d_value in self.depends:
            if d is None:
                return d
            if d_keyword not in d:
                return d
            if d[d_keyword] not in d_value:
                return d

        # print() the form
        print(self)

        # if we got a json seed, use its values as default
        # else use the Keyword object default
        if d and self.keyword in d:
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
                if default is None and self.required:
                    print("value required")
                    continue
                # keyword is optional, leave dictionary untouched
                return d
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
                # loop for more key@<scope> = values
                print("More '%s' ? <enter> to step to the next parameter."%self.keyword)
                continue
            else:
                return d

class Section(object):
    def __init__(self, section, top=None):
        self.section = section
        self.top = top
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

    def template(self, fmt="text"):
        k = self.getkey("type")
        if k is None:
            return self._template(fmt=fmt)
        if k.candidates is None:
            return self._template(fmt=fmt)
        s = ""
        if not k.strict_candidates:
            s += self._template(fmt=fmt)
        for t in k.candidates:
            s += self._template(t, fmt=fmt)
        return s

    def _template(self, rtype=None, fmt="text"):
        section = self.section
        if self.section in self.top.deprecated_sections:
            return ""
        if rtype and self.section+"."+rtype in self.top.deprecated_sections:
            return ""
        if fmt == "text":
            return self._template_text(rtype, section)
        elif fmt == "rst":
            return self._template_rst(rtype, section)
        else:
            return ""

    def _template_text(self, rtype, section):
        dpath = rcEnv.paths.pathdoc
        fpath = os.path.join(dpath, self.top.template_prefix+section+".conf")
        if rtype:
            section += ", type "+rtype
            fpath = os.path.join(dpath, self.top.template_prefix+self.section+"."+rtype+".conf")
        s = "#"*78 + "\n"
        s += "# %-74s #\n" % " "
        s += "# %-74s #\n" % section
        s += "# %-74s #\n" % " "
        s += "#"*78 + "\n\n"
        if section in self.top.base_sections:
            s += "[%s]\n" % self.section
        else:
            s += "[%s#0]\n" % self.section
        if rtype is not None:
            s += ";type = " + rtype + "\n\n"
        for keyword in sorted(self.getkeys(rtype)):
            s += keyword.template(fmt="text")
        for keyword in sorted(self.getprovkeys(rtype)):
            s += keyword.template(fmt="text")
        if rtype is not None:
            for keyword in sorted(self.getkeys()):
                if keyword.keyword == "type":
                    continue
                s += keyword.template(fmt="text")
        with open(fpath, "w") as f:
            f.write(s)
        return s

    def _template_rst(self, rtype, section):
        dpath = os.path.join(rcEnv.paths.pathtmp, "rst")
        if not os.path.exists(dpath):
            os.makedirs(dpath)
        if rtype:
            section += "."+rtype
            fpath = os.path.join(dpath, self.top.template_prefix+self.section+"."+rtype+".rst")
        else:
            fpath = os.path.join(dpath, self.top.template_prefix+section+".rst")
        s = section + "\n"
        s += "*" * len(section) + "\n\n"
        if self.top.template_prefix != "template.node." and len(section.split('.')) > 1:
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
        with open(fpath, "w") as f:
            f.write(s)
        return s

    def getkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None and not k.provisioning]
        else:
            return [k for k in self.keywords if k.rtype and rtype in k.rtype and not k.provisioning]

    def getprovkeys(self, rtype=None):
        if rtype is None:
            return [k for k in self.keywords if k.rtype is None and k.provisioning]
        else:
            return [k for k in self.keywords if k.rtype and rtype in k.rtype and k.provisioning]

    def getkey(self, keyword, rtype=None):
        if '@' in keyword:
            l = keyword.split('@')
            if len(l) != 2:
                return
            keyword, node = l
        if rtype:
            fkey = ".".join((self.section, rtype, keyword))
            if fkey in self.top.deprecated_keywords:
                keyword = self.top.deprecated_keywords[fkey]
                if keyword is None:
                    return
            for k in self.keywords:
                if k.keyword != keyword:
                    continue
                if k.rtype is None:
                    return k
                elif isinstance(k.rtype, list) and rtype in k.rtype:
                    return k
                elif rtype == k.rtype:
                    return k
        else:
            fkey = ".".join((self.section, keyword))
            if fkey in self.top.deprecated_keywords:
                keyword = self.top.deprecated_keywords[fkey]
            for k in self.keywords:
                if k.keyword == keyword:
                    return k
        return

class KeywordStore(dict):
    def __init__(self, provision=False, deprecated_keywords={}, deprecated_sections={},
                 template_prefix="template.", base_sections=[], has_default_section=True):
        self.sections = {}
        self.deprecated_sections = deprecated_sections
        self.deprecated_keywords = deprecated_keywords
        self.template_prefix = template_prefix
        self.base_sections = base_sections
        self.provision = provision
        self.has_default_section = has_default_section

    def __iadd__(self, o):
        if not isinstance(o, Keyword):
            return self
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
            return Section(k)
        return self.sections[str(key)]

    def __str__(self):
        s = ''
        for section in self.sections:
            s += str(self.sections[section])
        return s

    def print_templates(self, fmt="text"):
        for section in sorted(self.sections.keys()):
            print(self.sections[section].template(fmt=fmt))

    def required_keys(self, section, rtype=None):
        if section not in self.sections:
            return []
        return [k for k in sorted(self.sections[section].getkeys(rtype)) if k.required is True]

    def purge_keywords_from_dict(self, d, section):
        if section == "env":
            return d
        if 'type' in d:
            rtype = d['type']
        else:
            rtype = None
        delete_keywords = []
        for keyword, value in d.items():
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
        """ Given a resource dictionary, spot missing required keys
            and provide a new dictionary to merge populated by default
            values
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
            if key.keyword in map(lambda x: x.split('@')[0], d.keys()):
                continue
            if key.default is None:
                raise MissKeyNoDefault("No default value for required key '%s' in section '%s'"%(key.keyword, rid))
            print("Implicitely add [%s] %s = %s" % (rid, key.keyword, str(key.default)))
            completion[key.keyword] = key.default

        # purge unknown keywords and provisioning keywords
        completion = self.purge_keywords_from_dict(completion, section)

        return completion

    def form_sections(self, sections):
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

            # purge the provisioning keywords
            sections[rid] = self.purge_keywords_from_dict(sections[rid], section)

        return defaults, sections


