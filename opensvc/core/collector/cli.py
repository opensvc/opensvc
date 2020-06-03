# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import stat
import json
import optparse
import shlex
import re
import copy
import textwrap

# issue19884 workaround (spurious heading '\033[1034h')
TERM = os.environ.get("TERM")
if TERM:
    del os.environ["TERM"]
    import readline
    os.environ["TERM"] = TERM

import atexit
import fnmatch

from foreign.six.moves import configparser as ConfigParser
import core.exceptions as ex
from foreign.six.moves import input
from utilities.storage import Storage
from utilities.proc import find_editor
from utilities.render.color import formatter
from utilities.string import bdecode, is_glob

try:
    import requests
except ImportError:
    raise ex.Error("This feature requires the python requests module")

try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
except ImportError:
    InsecureRequestWarning = None

# the collector api doc uses restructured text we'll have to print
# in the command help messages
try:
    import docutils.utils
    import docutils.parsers
    import docutils.parsers.rst
    has_docutils = True
except ImportError:
    has_docutils = False

progname = "opensvc-cli"
homedir = os.path.expanduser("~")
api_cache_f = os.path.join(homedir, "."+progname+".api")
conf_f = os.path.join(homedir, "."+progname)
conf_section = "collector"
history_f = conf_f + "_history"
global path
path = "/"
api_cache = None

ls_info_default = {
    "filter": "id",
    "props": ["id"],
    "fmt": "%(id)-10s",
}
ls_info = {
  "": {
    "filter": "path",
    "props": ["path"],
    "fmt": "           %(path)s",
  },
  "action_queue": {
    "filter": "id",
    "props": ["id", "command"],
    "fmt": "%(id)-10s %(command)s",
  },
  "apps": {
    "filter": "app",
    "props": ["id", "app"],
    "fmt": "%(id)-10s %(app)s",
  },
  "nodes": {
    "filter": "nodename",
    "props": ["node_id", "nodename"],
    "fmt": "%(node_id)s %(nodename)s",
  },
  "services": {
    "filter": "svcname",
    "props": ["svc_id", "svcname"],
    "fmt": "%(svc_id)s %(svcname)s",
  },
  "rulesets": {
    "filter": "ruleset_name",
    "props": ["id", "ruleset_name"],
    "fmt": "%(id)-10s %(ruleset_name)s",
  },
  "modulesets": {
    "filter": "modset_name",
    "props": ["id", "modset_name"],
    "fmt": "%(id)-10s %(modset_name)s",
  },
  "users": {
    "filter": "email",
    "props": ["id", "email"],
    "fmt": "%(id)-10s %(email)s",
  },
  "groups": {
    "filter": "role",
    "props": ["id", "role"],
    "fmt": "%(id)-10s %(role)s",
  },
  "tags": {
    "filter": "tag_name",
    "props": ["tag_id", "tag_name"],
    "fmt": "%(tag_id)s %(tag_name)s",
  },
  "variables": {
    "filter": "var_name",
    "props": ["id", "var_name"],
    "fmt": "%(id)-10s %(var_name)s",
  },
  "modules": {
    "filter": "modset_mod_name",
    "props": ["id", "modset_mod_name"],
    "fmt": "%(id)-10s %(modset_mod_name)s",
  },
  "filters": {
    "filter": "f_label",
    "props": ["id", "f_label"],
    "fmt": "%(id)-10s %(f_label)s",
  },
  "filtersets": {
    "filter": "fset_name",
    "props": ["id", "fset_name"],
    "fmt": "%(id)-10s %(fset_name)s",
  },
}


#
# requests setup
#
try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass

class Cmd(object):
    command = "undef"
    desc = ""
    parser = None
    candidates_path = {}
    api_candidates = False
    parser_options = []

    # color codes
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

    def __init__(self, cli=None):
        self.cli = cli
        self.options = Storage()
        self.options.format = cli.format if cli else None

    @formatter
    def print_content(self, s):
        data = json.loads(bdecode(s))
        if self.options.format == "json":
            return data
        if "info" in data:
            if isinstance(data["info"], list):
                infos = data["info"]
            else:
                infos = [data["info"]]
            for info in infos:
                print("Info:", info)
        if "error" in data:
            if isinstance(data["error"], list):
                errors = data["error"]
            else:
                errors = [data["error"]]
            for error in errors:
                print("Error:", error)
        if "data" in data:
            return data["data"]
        return ""

    def path_match_handlers(self, p):
        for a, l in self.cli.api_o.get().items():
            for d in l:
                if path_match_handler(p, d):
                    return True
        return False

    def path_match_handlers_or_parents(self, p):
        for a, l in self.cli.api_o.get().items():
            for d in l:
                if path_match_handler_or_parents(p, d):
                    return True
        return False

    def colorize(self, s, c=None):
        if c is None:
            return s
        return c + s + self.END

    def get_handler(self, action, p):
        for d in self.cli.api_o.get()[action]:
            if path_match_handler(p, d):
                return d

    def match(self, line):
        """
          Tells if a CLI command line is to be handled by this command class,
          using the first word as a telltale.
        """
        l = line.split()
        if len(l) == 0 or l[0] != self.command:
            return False
        return True

    def replace_params_in_path(self, candidates_path, words):
        d = {}
        if candidates_path.count("<") == 0:
            return candidates_path
        for i, w in enumerate(words):
            if i < len(words) - 1:
                next_word = words[i+1]
            else:
                next_word = None
            if w.startswith("--") and next_word is not None:
                d[w.lstrip("-")] = next_word
        p = copy.copy(candidates_path)
        new_p = copy.copy(candidates_path)
        while p.count("<") > 0:
            try:
                param = p[p.index("<")+1:p.index(">")]
                p = p[p.index(">")+1:]
                if param in d:
                    new_p = new_p.replace("<"+param+">", d[param])
            except:
                break
        return new_p

    def set_parser_options_from_cmdline(self, line):
        words = shlex.split(line)
        self.set_parser_options_from_words(words)

    def set_parser_options_from_words(self, words):
        if len(words) == 0:
            return
        _path = None
        for i, w in enumerate(words):
            if i == 0:
                # command word
                continue
            if i == 1 and not w.startswith("-"):
                _path = w
                break
            if i > 1 and not words[i-1].startswith("-"):
                _path = w
                break
        if _path is None:
            return
        try:
            self.set_parser_options(_path)
        except Exception as e:
            print(e)

    def candidates(self, pattern, words):
        candidates = []
        if hasattr(self, "candidates_path"):
            param = words[-1]
            if not param in self.candidates_path and len(words) > 2 and pattern != "":
                param = words[-2]
            candidates_path = self.candidates_path.get(param)
        else:
            candidates_path = None
        if candidates_path:
            candidates_path = self.replace_params_in_path(candidates_path, words)
            pattern = candidates_path + "/" + pattern
        else:
            self.set_parser_options_from_words(words)
            for o in self.parser.option_list:
                candidates += o._long_opts

        if pattern is None:
            pattern = ""
        elif pattern == ".." or pattern.endswith("/.."):
            pass
        elif pattern.startswith("/") and pattern.endswith("/"):
            pass
        elif pattern.count("/") == 0:
            pattern += "*"
        else:
            pattern = pattern[:pattern.rindex("/")+1]
        ls_data = self.ls_data(pattern)
        for e in ls_data:
            if e.startswith("OBJ"):
                candidate = e.replace("OBJ", "").strip()
                l = candidate.split()
                if len(l) > 1 and len(candidate) > 11:
                    # <id 10-char padded> <string>
                    candidate = candidate[11:]
                if candidates_path is None and pattern.count("/") > 0:
                    candidate = pattern.rstrip("/")+"/"+candidate
                candidates.append(candidate)
            elif candidates_path is None and e.startswith("API") and e != "API":
                candidate = e.split()[-1]
                if pattern.startswith("/") and not candidate.startswith("/"):
                    candidate = pattern + candidate
                candidates.append(candidate)
        return candidates

    def args_to_path(self, args):
        try:
            arg1 = args[1]
            if arg1.startswith("/"):
                _path = arg1
            else:
                _path = copy.copy(path) + "/" + args[1]
        except:
            _path = copy.copy(path)
        return _path

    def get_data_from_options(self, options):
        data = {}
        files = {}
        headers = None
        if options is None or "data" not in options.__dict__ or options.data is None:
            return data, files, headers
        for d in options.data:
            if len(d) > 0 and d[0] == "@" and os.path.exists(d[1:]):
                with open(d[1:], 'r') as fd:
                    data = fd.read()
                try:
                    data = data.encode("utf-8")
                except:
                    pass
                headers = {
                  'Accept' : 'application/json',
                  'Content-Type' : 'application/json; charset=utf-8'
                }
                return data, files, headers
            if d.count("=") == 0 or len(d) < d.index("=")+1:
                print("ignore malformated data:", d)
                continue
            key = d[:d.index("=")]
            val = d[d.index("=")+1:]
            if len(val) > 1 and val[0] == "@" and os.path.exists(val[1:]):
                fpath = val[1:]
                try:
                    fd = open(fpath, 'rb')
                except:
                    print("error opening file %s" % fpath, file=sys.stderr)
                    raise
                files["file"] = (os.path.realpath(fpath), fd)
            else:
                data[key] = val
        return data, files, headers

    def factorize_dot_dot(self, p):
        l1 = p.split("/")
        l2 = []
        for i, e in enumerate(l1):
            if i == 0:
                l2.append(e)
                continue
            if e == "..":
                l2.pop()
                continue
            l2.append(e)
        if l2 == [""]:
            p = "/"
        else:
            p = "/".join(l2)
        if len(p) > 1:
            p.rstrip("/")
        return p

    def ls_data(self, line):
        ls_data = []
        global path
        line = line.strip()

        # strip the ls command
        relpath = re.sub(r"^\s*ls\s+", "", line)
        if relpath == "ls":
            relpath = ""

        relpath = relpath.strip()
        if relpath == ".." or relpath.endswith("/.."):
            relpath += "/"

        p = get_fullpath(relpath)
        if p.count("/") == 0:
            relpath = ""
            raw_req_path = copy.copy(path)
            shell_pattern = p
        elif is_glob(p[p.rindex("/"):]):
            v = p.split("/")
            raw_req_path = "/".join(v[:-1])
            shell_pattern = v[-1]
            v = relpath.split("/")
            relpath = "/".join(v[:-1])
        else:
            raw_req_path = p
            shell_pattern = ""

        req_path = self.factorize_dot_dot(raw_req_path)
        sql_pattern = shell_pattern.replace("*", "%")
        sql_pattern = sql_pattern.replace("?", "_")

        last = req_path.rstrip("/").split("/")[-1]
        info = ls_info.get(last, ls_info_default)
        props = info.get("props", [])
        filter_prop = info.get("filter", "id")
        fmt = info.get("fmt", "%(id)s")

        if req_path not in ("/", "") and self.path_match_handlers(req_path):
            # object listing
            params = {
              "limit": 0,
              "meta": 0,
              "props": ",".join(props),
            }
            if len(sql_pattern) > 0:
                params["query"] = filter_prop + " like " + sql_pattern
            r = requests.get(self.cli.api+req_path, params=params, auth=self.cli.auth, verify=not self.cli.insecure)
            validate_response(r)
            data = json.loads(bdecode(r.content)).get("data")
            if type(data) == list:
                ls_data += map(lambda d: "OBJ " + fmt % d, data)

        if self.api_candidates:
            # api paths listing
            info = ls_info.get("", ls_info_default)
            fmt = info.get("fmt", "%(id)s")
            props = info.get("props", [])
            filter_prop = info.get("filter", [])
            data = [d for d in self.get_handler_paths() if path_match_handler_or_parents(req_path, d) and d["path"] != req_path]
            #data += path_children_api(req_path)
            if len(shell_pattern) > 0:
                if not shell_pattern.startswith("/"):
                    shell_pattern = req_path + "/" + shell_pattern
                    shell_pattern = shell_pattern.replace("//", "/")
                data = [d for d in data if fnmatch.fnmatch(d.get(filter_prop), shell_pattern)]
            for i, d in enumerate(data):
                data[i]["path"] = re.sub("^"+req_path, relpath, d["path"])

            for d in data:
                line = "API "
                for action in ("GET", "POST", "DELETE", "PUT"):
                    if action in d["actions"]:
                        line += action
                    else:
                        line += "."*len(action)
                    line += " "
                line += fmt %d
                ls_data.append(line)
        return ls_data

    def get_handler_paths(self):
        data = self.cli.api_o.get()
        all_handlers = {}
        for action, l in data.items():
            for h in l:
                if h["path"] not in all_handlers:
                    h["actions"] = [action]
                    all_handlers[h["path"]] = h
                else:
                    all_handlers[h["path"]]["actions"].append(action)
        return [all_handlers[p] for p in sorted(all_handlers.keys())]

    def set_parser_options(self, _path):
        if not _path.startswith("/"):
            _path = os.path.join(path, _path)
        try:
            h = self.get_handler(self.command.upper(), _path)
        except Exception as e:
            return
        if h is None:
            return self.parser
        for o in self.parser._get_all_options()[1:]:
            self.parser.remove_option(str(o))
        if hasattr(self, "parser_options"):
            for o in self.parser_options:
                self.parser.add_option(o)
        for param, d in h["params"].items():
            if d.get("type") == "list":
                action = "append"
                default = []
            else:
                action = "append"
                default = None
            self.parser.add_option("--"+param, default=default, action=action, dest=param, help=d["desc"])

class IndentedHelpFormatterRst(optparse.IndentedHelpFormatter):
    def format_description(self, description):
        if not has_docutils:
            return description

        if not description:
            return ""

        doc = docutils.utils.new_document("foo")
        doc.settings.tab_width = 4
        doc.settings.pep_references = None
        doc.settings.rfc_references = None
        p = docutils.parsers.rst.Parser()
        p.parse(description, doc)
        description = doc.astext()

        desc_width = self.width - self.current_indent
        indent = " "*self.current_indent

        # the above is still the same
        bits = description.split('\n')
        formatted_bits = [
          textwrap.fill(bit, desc_width, initial_indent=indent, subsequent_indent=indent)
          for bit in bits
        ]
        result = "\n".join(formatted_bits) + "\n"
        return result

    def format_option(self, option):
        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else: # start help on same line as opts
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option)
            # Everything is the same up through here
            help_lines = []
            for para in help_text.split("\n"):
                help_lines.extend(textwrap.wrap(para, self.help_width))
            # Everything is the same after here
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line) for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)


class CliError(Exception):
    pass

class Api(object):
    api_cache = None

    def __init__(self, cli=None, refresh=False):
        self.cli = cli
        self.load(refresh=refresh)

    def load(self, refresh=False):
        if not refresh and os.path.exists(api_cache_f):
            # try local cache first
            try:
                with open(api_cache_f, 'r') as f:
                    self.api_cache = json.loads(f.read())
                return
            except Exception as e:
                print(e)
                os.unlink(api_cache_f)

        # fallback to fetching the cache
        print("load api cache", file=sys.stderr)
        r = requests.get(self.cli.api, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        try:
            self.api_cache = json.loads(bdecode(r.content))["data"]
        except:
            raise CliError(r.content)

        # update local cache
        with open(api_cache_f, 'w') as f:
            f.write(json.dumps(self.api_cache, indent=4))

    def get(self):
        if self.api_cache:
            return copy.deepcopy(self.api_cache)
        self.load()
        return copy.deepcopy(self.api_cache)

class OptionParsingError(RuntimeError):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class OptionParsingExit(Exception):
    def __init__(self, status, msg):
        self.msg = msg
        self.status = status
    def __str__(self):
        return self.msg

class CmdOptionParser(optparse.OptionParser):
    def __init__(self, *args, **vars):
        vars["formatter"] = IndentedHelpFormatterRst()
        optparse.OptionParser.__init__(self, *args, **vars)

    def error(self, msg):
        raise OptionParsingError(msg)

    def exit(self, status=0, msg=None):
        raise OptionParsingExit(status, msg)

def get_fullpath(relpath):
    if relpath.startswith("/"):
        return relpath
    if relpath == "":
        return path
    return path+"/"+relpath

class CmdHelp(Cmd):
    command = "help"
    desc = "Print this help message."
    parser = CmdOptionParser(description=desc)

    def cmd(self, line):
        from textwrap import TextWrapper
        wrapper = TextWrapper(initial_indent="    ", subsequent_indent="    ", width=78)
        commands_h = {}
        for c in self.cli.commands:
            commands_h[c.command] = c
        base_commands = sorted(commands_h.keys())
        for command in base_commands:
            c = commands_h[command]
            print(c.command)
            print()
            if hasattr(c, "desc"):
                print(wrapper.fill(c.desc))
                print()

class CmdLs(Cmd):
    api_candidates = True
    command = "ls"
    desc = "List the API handlers and available objects matching the given pattern."
    parser = CmdOptionParser(description=desc)

    def cmd(self, line):
        ls_data = self.ls_data(line)
        for s in ls_data:
            print(s)

class CmdDelete(Cmd):
    api_candidates = True
    command = "delete"
    desc = "Execute a DELETE request on the given API handler."
    parser = CmdOptionParser(description=desc)
    parser_options = [
      optparse.make_option("--data", default=None, action="append", dest="data",
                           help="A key=value pair to filter the deleted data. Multiple --data can be specified.")
    ]

    def cmd(self, line):
        self.set_parser_options_from_cmdline(line)
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        data, files, headers = self.get_data_from_options(options)
        params = {}
        if 'filters' in options.__dict__ and options.filters:
            params["filters"] = options.filters
        if 'query' in options.__dict__ and options.query is not None:
            params["query"] = options.query
        if 'limit' in options.__dict__ and options.limit:
            params["limit"] = int(options.limit[0])
        _path = self.args_to_path(args)
        r = requests.delete(self.cli.api+_path, params=params, data=data, headers=headers, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

class CmdPost(Cmd):
    api_candidates = True
    command = "post"
    desc = "Execute a POST request on the given API handler. The data can be set using --data."
    parser = CmdOptionParser(description=desc)
    parser_options = [
      optparse.make_option("--data", default=None, action="append", dest="data",
                           help="A key=value pair to include in the post data. Multiple --data can be specified.")
    ]

    def cmd(self, line):
        self.set_parser_options_from_cmdline(line)
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        data, files, headers = self.get_data_from_options(options)
        params = {}
        if 'filters' in options.__dict__ and options.filters:
            params["filters"] = options.filters
        if 'query' in options.__dict__ and options.query is not None:
            params["query"] = options.query
        if 'limit' in options.__dict__ and options.limit:
            params["limit"] = int(options.limit[0])
        _path = self.args_to_path(args)
        r = requests.post(self.cli.api+_path, data=data, files=files, params=params, headers=headers, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

class CmdPut(Cmd):
    api_candidates = True
    command = "put"
    desc = "Execute a PUT request on the given API handler. The data can be set using --data."
    parser = CmdOptionParser(description=desc)
    parser_options = [
      optparse.make_option("--data", default=None, action="append", dest="data",
                           help="A key=value pair to include in the post data. Multiple --data can be specified.")
    ]

    def cmd(self, line):
        self.set_parser_options_from_cmdline(line)
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        data, files, headers = self.get_data_from_options(options)
        _path = self.args_to_path(args)
        r = requests.put(self.cli.api+_path, data=data, files=files, headers=headers, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

class CmdSafe(Cmd):
    command = "safe"
    desc = "Upload, download and manage files in the collector safe. The safe is a file sharing facility with access control rules for nodes and users making it suitable to serve compliance reference files."
    parser = CmdOptionParser(description=desc)
    parser.add_option("--ls", default=None, action="store_true", dest="ls",
                      help="List the accessible files in the safe.")
    parser.add_option("--upload", default=None, action="store_true", dest="upload",
                      help="Upload the file pointed by --file to the safe. Optionally give a name using --name.")
    parser.add_option("--download", default=None, action="store_true", dest="download",
                      help="Download from the safe the file pointed by --file to the file path or directory pointed by --to.")
    parser.add_option("--file", default=None, action="store", dest="file",
                      help="The safe file uuid to download, or the local file to upload.")
    parser.add_option("--to", default=None, action="store", dest="to",
                      help="The local file path or directory name to download.")
    parser.add_option("--name", default=None, action="store", dest="name",
                      help="The user-friendly name to attach to the upload.")
    parser.add_option("--id", default=None, action="store", dest="id", type="int",
                      help="An optional safe file integer id. If specified the safe id will point to the new uploaded version of the file. The previous version is still referenced by the same uuid.")
    candidates_path = {
      "--file": "/safe",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.ls(options)
        self.upload(options)
        self.download(options)

    def ls(self, options):
        if options.ls is None:
            return
        params = {}
        r = requests.get(self.cli.api+"/safe", params=params, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def upload(self, options):
        if options.upload is None:
            return
        if options.file is None:
            raise CliError("--file is mandatory for --upload")
        data = {}
        if options.name:
            data["name"] = options.name
        if not os.path.exists(options.file):
            raise CliError("%s file not found" % options.file)

        if options.id is not None:
            path = "/safe/%d/upload" % options.id
        else:
            path = "/safe/upload"

        files = {
          "file": (os.path.realpath(options.file), open(options.file, 'rb')),
        }

        r = requests.post(self.cli.api+path, data=data, files=files, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def download(self, options):
        if options.download is None:
            return
        if options.file is None:
            raise CliError("--file is mandatory for --download")
        if options.to is None:
            raise CliError("--to is mandatory for --download")

        if os.path.exists(options.to) and os.path.isdir(options.to):
            to = os.path.join(options.to, options.file)
        else:
            to = options.to

        r = requests.get(self.cli.api+"/safe/"+options.file+"/download", stream=True, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)

        if not r.ok:
            try:
                d = json.loads(bdecode(r.content))
                print(d["error"], file=sys.stderr)
                return
            except:
                pass
            raise CliError("download failed")

        with open(options.to, 'wb') as f:
            pass
        os.chmod(options.to, 0o0600)
        with open(options.to, 'wb') as f:
            for block in r.iter_content(1024):
                print(".")
                f.write(block)

        print("downloaded")



class CmdSysreport(Cmd):
    command = "sysreport"
    desc = "Show sysreport information"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--log", default=None, action="store_true", dest="log",
                      help="")
    parser.add_option("--begin", default=None, action="store", dest="begin",
                      help="The sysreport analysis begin date.")
    parser.add_option("--end", default=None, action="store", dest="end",
                      help="The sysreport analysis begin date.")
    parser.add_option("--path", default=None, action="store", dest="path",
                      help="A path globing pattern to limit the sysreport analysis to.")
    parser.add_option("--node", default=None, action="store", dest="node",
                      help="The sysreport node name.")
    parser.add_option("--cid", default=None, action="store", dest="cid",
                      help="The commit id to show as diff. This cid is displayed in the summary listing obtained by the --log action without specifying --cid.")
    candidates_path = {
      "--node": "/nodes",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.log(options)
        self.log_cid(options)

    def print_log(self, data):
        for d in data["data"]:
            print(self.colorize("cid: %s" % d["cid"], c=self.DARKCYAN))
            print(self.colorize("change detection date: %s" % d["start"].replace("T", " "), c=self.GREEN))
            if "summary" in d:
                print()
                print("  "+d["summary"])
            print()
            for fpath in d["stat"]:
                print("  "+fpath)
            print()

    def print_log_cid(self, data):
        for fpath, d in data["data"]["blocks"].items():
            print(self.colorize("path: " + fpath, c=self.DARKCYAN))
            if d["secure"]:
                print(self.colorize("visible: by node responsibles", c=self.RED))
            else:
                print(self.colorize("visible: by everyone", c=self.DARKCYAN))
            print()
            for line in d["diff"].split("\n"):
                if line.startswith("-"):
                    c = self.RED
                elif line.startswith("+"):
                    c = self.GREEN
                else:
                    c = None
                print(self.colorize(line, c=c))
            print()

    def log(self, options):
        if options.log is None or options.node is None or options.cid is not None:
            return
        params = {}
        if options.begin:
            params["begin"] = options.begin
        if options.end:
            params["end"] = options.end
        if options.path:
            params["path"] = options.path
        r = requests.get(self.cli.api+"/nodes/"+options.node+"/sysreport", params=params, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        data = json.loads(bdecode(r.content))
        self.print_log(data)

    def log_cid(self, options):
        if options.log is None or options.node is None or options.cid is None:
            return
        params = {}
        if options.path:
            params["path"] = options.path
        r = requests.get(self.cli.api+"/nodes/"+options.node+"/sysreport/"+options.cid, params=params, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        data = json.loads(bdecode(r.content))
        self.print_log_cid(data)


class CmdFilter(Cmd):
    command = "filter"
    desc = "Handle design actions on a filter"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--list", default=None, action="store_true", dest="list",
                      help="List filters")
    parser.add_option("--show", default=None, action="store_true", dest="show",
                      help="Show a filter design")
    parser.add_option("--create", default=None, action="store_true", dest="create",
                      help="Create a filter")
    parser.add_option("--delete", default=None, action="store_true", dest="delete",
                      help="Delete a filter")
    parser.add_option("--set", default=None, action="store_true", dest="set",
                      help="Set filter properties")
    parser.add_option("--attach", default=None, action="store_true", dest="attach",
                      help="Attach a filter to the filterset pointed by --filterset")
    parser.add_option("--detach", default=None, action="store_true", dest="detach",
                      help="Detach a filter from the filterset pointed by --filterset")
    parser.add_option("--filter", default=None, action="store", dest="filter",
                      help="The name or id of the filter to manage")
    parser.add_option("--filterset", default=None, action="store", dest="filterset",
                      help="The name or id of the filterset to attach to or detach from")
    parser.add_option("--value", default=None, action="store", dest="value",
                      help="with --set or --create, set the filter value parameter")
    parser.add_option("--operator", default=None, action="store", dest="operator",
                      help="with --set or --create, set the filter operator parameter. Accepted operators: =, <, >, <=, >=, LIKE, IN")
    parser.add_option("--field", default=None, action="store", dest="field",
                      help="with --set or --create, set the filter field parameter")
    parser.add_option("--table", default=None, action="store", dest="table",
                      help="with --set or --create, set the filter table parameter")
    parser.add_option("--order", default=None, action="store", dest="order",
                      help="with --attach, set the filter attachment order parameter. Integer.")
    parser.add_option("--logical-operator", default=None, action="store", dest="logical_operator",
                      help="with --attach, set the filter attachment logical operator parameter. Accepted operators: AND, OR, AND NOT, OR NOT")
    candidates_path = {
      "--filterset": "/filtersets",
      "--filter": "/filters",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        if options.filter:
            options.filter = options.filter.replace("%", "(percent)")
        self.list_filters(options)
        self.show_filter(options)
        self.create_filter(options)
        self.delete_filter(options)
        self.set_filter(options)
        self.attach_filter_to_filterset(options)
        self.detach_filter_from_filterset(options)

    def list_filters(self, options):
        if options.list is None:
            return
        p = "/filters"
        CmdLs(self.cli).cmd(p)

    def show_filter(self, options):
        if options.show is None or options.filter is None:
            return
        _path = "/filters/"+options.filter
        r = requests.get(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        data = json.loads(bdecode(r.content))
        self.print_filter(data["data"][0])

    def print_filter(self, data):
        print(ls_info.get("filters").get("fmt") % data)

    def create_filter(self, options):
        if options.create is None:
            return
        global path
        _path = "/filters"
        data = {
          "f_table": options.table,
          "f_field": options.field,
          "f_op": options.operator,
          "f_value": options.value,
        }
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def delete_filter(self, options):
        if options.delete is None or options.filter is None:
            return
        global path
        _path = "/filters/"+options.filter
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_filter_to_filterset(self, options):
        if options.attach is None or options.filterset is None or options.filter is None:
            return
        _path = "/filtersets/%s/filters/%s" % (options.filterset, options.filter)
        data = {}
        if options.logical_operator:
            data["f_log_op"] = options.logical_operator
        if options.logical_operator:
            data["f_order"] = options.order
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_filter_from_filterset(self, options):
        if options.detach is None or options.filterset is None or options.filter is None:
            return
        _path = "/filtersets/%s/filters/%s" % (options.filterset, options.filter)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_filter(self, options):
        if options.set is None or options.filter is None:
            return
        data = {}
        if options.table is not None:
            data["f_table"] = options.table
        if options.operator is not None:
            data["f_op"] = options.operator
        if options.table is not None:
            data["f_field"] = options.field
        if options.value is not None:
            data["f_value"] = options.value
        if len(data) == 0:
            return
        _path = "/filters/%s" % options.filter
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)


class CmdFilterset(Cmd):
    command = "filterset"
    desc = "Handle design actions on a filterset"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--list", default=None, action="store_true", dest="list",
                      help="List filtersets")
    parser.add_option("--show", default=None, action="store_true", dest="show",
                      help="Show a filterset design, with nesting")
    parser.add_option("--create", default=None, action="store_true", dest="create",
                      help="Create a filterset")
    parser.add_option("--delete", default=None, action="store_true", dest="delete",
                      help="Delete a filterset")
    parser.add_option("--set", default=None, action="store_true", dest="set",
                      help="Set filterset properties")
    parser.add_option("--attach", default=None, action="store_true", dest="attach",
                      help="Attach a filterset to the filterset pointed by --parent-filterset")
    parser.add_option("--detach", default=None, action="store_true", dest="detach",
                      help="Detach a filterset from the filterset pointed by --parent-filterset")
    parser.add_option("--rename", default=None, action="store_true", dest="rename",
                      help="Rename a filterset")
    parser.add_option("--filterset", default=None, action="store", dest="filterset",
                      help="The name or id of the filterset to manage")
    parser.add_option("--parent-filterset", default=None, action="store", dest="parent_filterset",
                      help="The name or id of the filterset to attach to or detach from")
    parser.add_option("--to", default=None, action="store", dest="to",
                      help="with --rename, set the new filterset name")
    parser.add_option("--stats", default=None, action="store_true", dest="stats",
                      help="with --set, set the filterset stats parameter to true")
    parser.add_option("--not-stats", default=None, action="store_false", dest="stats",
                      help="with --set, set the filterset stats parameter to false")
    candidates_path = {
      "--to": "/filtersets",
      "--filterset": "/filtersets",
      "--parent-filterset": "/filtersets",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.list_filtersets(options)
        self.show_filterset(options)
        self.create_filterset(options)
        self.delete_filterset(options)
        self.set_filterset(options)
        self.attach_filterset_to_filterset(options)
        self.detach_filterset_from_filterset(options)
        self.rename_filterset(options)

    def list_filtersets(self, options):
        if options.list is None:
            return
        p = "/filtersets"
        CmdLs(self.cli).cmd(p)

    def show_filterset(self, options):
        if options.show is None or options.filterset is None:
            return
        o = CmdShow(self.cli)
        data = o.get_data("/filtersets/"+options.filterset)
        o.print_filterset(options.filterset, data)

    def create_filterset(self, options):
        if options.create is None or options.filterset is None:
            return
        global path
        _path = "/filtersets"
        data = {
          "fset_name": options.filterset,
        }
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def delete_filterset(self, options):
        if options.delete is None or options.filterset is None:
            return
        global path
        _path = "/filtersets/"+options.filterset
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_filterset_to_filterset(self, options):
        if options.attach is None or options.parent_filterset is None or options.filterset is None:
            return
        _path = "/filtersets/%s/filtersets/%s" % (options.parent_filterset, options.filterset)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_filterset_from_filterset(self, options):
        if options.detach is None or options.parent_filterset is None or options.filterset is None:
            return
        _path = "/filtersets/%s/filtersets/%s" % (options.parent_filterset, options.filterset)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def rename_filterset(self, options):
        if options.rename is None or options.filterset is None or options.to is None:
            return
        data = {
          "fset_name": options.to,
        }
        _path = "/filtersets/%s" % options.filterset
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_filterset(self, options):
        if options.set is None or options.filterset is None:
            return
        self.set_filterset_stats(options)

    def set_filterset_stats(self, options):
        if options.stats is None:
            return
        data = {
          "fset_stats": options.stats,
        }
        _path = "/filtersets/%s" % options.filterset
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)


class CmdModuleset(Cmd):
    command = "moduleset"
    desc = "Handle design actions on a compliance moduleset"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--list", default=None, action="store_true", dest="list",
                      help="List modulesets")
    parser.add_option("--show", default=None, action="store_true", dest="show",
                      help="Show a moduleset design, with nesting")
    parser.add_option("--clone", default=None, action="store_true", dest="clone",
                      help="Clone a moduleset, including modules, moduleset-moduleset and moduleset-ruleset relations. Reset the publication as responsible groups.")
    parser.add_option("--create", default=None, action="store_true", dest="create",
                      help="Create a moduleset")
    parser.add_option("--delete", default=None, action="store_true", dest="delete",
                      help="Delete a moduleset")
    parser.add_option("--attach", default=None, action="store_true", dest="attach",
                      help="Attach the moduleset to a moduleset")
    parser.add_option("--detach", default=None, action="store_true", dest="detach",
                      help="Detach the moduleset from a moduleset")
    parser.add_option("--rename", default=None, action="store_true", dest="rename",
                      help="Rename a moduleset")
    parser.add_option("--moduleset", default=None, action="store", dest="moduleset",
                      help="The name or id of the moduleset to manage")
    parser.add_option("--parent-moduleset", default=None, action="store", dest="parent_moduleset",
                      help="The name or id of the moduleset to attach to or detach from")
    parser.add_option("--publication-group", default=None, action="store", dest="publication_group",
                      help="The name or id of the group to attach or detach as publication")
    parser.add_option("--responsible-group", default=None, action="store", dest="responsible_group",
                      help="The name or id of the group to attach or detach as responsible")
    parser.add_option("--to", default=None, action="store", dest="to",
                      help="with --rename, set the new moduleset name")
    candidates_path = {
      "--moduleset": "/compliance/modulesets",
      "--parent-moduleset": "/compliance/modulesets",
      "--publication-group": "/groups",
      "--responsible-group": "/groups",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.list_modulesets(options)
        self.clone_moduleset(options)
        self.show_moduleset(options)
        self.create_moduleset(options)
        self.delete_moduleset(options)
        self.attach_publication_group_to_moduleset(options)
        self.detach_publication_group_from_moduleset(options)
        self.attach_responsible_group_to_moduleset(options)
        self.detach_responsible_group_from_moduleset(options)
        self.attach_moduleset_to_moduleset(options)
        self.detach_moduleset_from_moduleset(options)
        self.rename_moduleset(options)

    def list_modulesets(self, options):
        if options.list is None:
            return
        p = "/compliance/modulesets"
        CmdLs(self.cli).cmd(p)

    def show_moduleset(self, options):
        if options.show is None:
            return
        o = CmdShow(self.cli)
        data = o.get_data("/compliance/modulesets/"+options.moduleset)
        o.print_moduleset(options.moduleset, data)

    def create_moduleset(self, options):
        if options.create is None or options.moduleset is None:
            return
        global path
        _path = "/compliance/modulesets"
        data = {
          "modset_name": options.moduleset,
        }
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def clone_moduleset(self, options):
        if options.clone is None or options.moduleset is None:
            return
        data = {
          "action": "clone",
        }
        _path = "/compliance/modulesets/%s" % options.moduleset
        r = requests.put(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def delete_moduleset(self, options):
        if options.delete is None or options.moduleset is None:
            return
        global path
        _path = "/compliance/modulesets/"+options.moduleset
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_publication_group_to_moduleset(self, options):
        if options.attach is None or options.publication_group is None or options.moduleset is None:
            return
        _path = "/compliance/modulesets/%s/publications/%s" % (options.moduleset, options.publication_group)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_responsible_group_to_moduleset(self, options):
        if options.attach is None or options.responsible_group is None or options.moduleset is None:
            return
        _path = "/compliance/modulesets/%s/responsibles/%s" % (options.moduleset, options.responsible_group)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_publication_group_from_moduleset(self, options):
        if options.detach is None or options.publication_group is None or options.moduleset is None:
            return
        _path = "/compliance/modulesets/%s/publications/%s" % (options.moduleset, options.publication_group)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_responsible_group_from_moduleset(self, options):
        if options.detach is None or options.responsible_group is None or options.moduleset is None:
            return
        _path = "/compliance/modulesets/%s/responsibles/%s" % (options.moduleset, options.responsible_group)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_moduleset_to_moduleset(self, options):
        if options.attach is None or options.parent_moduleset is None or options.moduleset is None:
            return
        _path = "/compliance/modulesets/%s/modulesets/%s" % (options.parent_moduleset, options.moduleset)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_moduleset_from_moduleset(self, options):
        if options.detach is None or options.parent_moduleset is None or options.moduleset is None:
            return
        _path = "/compliance/modulesets/%s/modulesets/%s" % (options.parent_moduleset, options.moduleset)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def rename_moduleset(self, options):
        if options.rename is None or options.moduleset is None or options.to is None:
            return
        data = {
          "modset_name": options.to,
        }
        _path = "/compliance/modulesets/%s" % options.moduleset
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)


class CmdModule(Cmd):
    command = "module"
    desc = "Handle design actions on a compliance module"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--list", default=None, action="store_true", dest="list",
                      help="List a moduleset modules")
    parser.add_option("--add", default=None, action="store_true", dest="add",
                      help="Add a module to a moduleset")
    parser.add_option("--remove", default=None, action="store_true", dest="remove",
                      help="Remove a module from a moduleset")
    parser.add_option("--set", default=None, action="store_true", dest="set",
                      help="Set module properties")
    parser.add_option("--rename", default=None, action="store_true", dest="rename",
                      help="Rename a module")
    parser.add_option("--module", default=None, action="store", dest="module",
                      help="The name or id of the module")
    parser.add_option("--moduleset", default=None, action="store", dest="moduleset",
                      help="The name or id of the module's moduleset")
    parser.add_option("--autofix", default=None, action="store_true", dest="autofix",
                      help="with --set, set the autofix property to true")
    parser.add_option("--not-autofix", default=None, action="store_false", dest="autofix",
                      help="with --set, set the autofix property to false")
    parser.add_option("--to", default=None, action="store", dest="to",
                      help="with --rename, set the new module name")
    candidates_path = {
      "--moduleset": "/compliance/modulesets",
      "--module": "/compliance/modulesets/<moduleset>/modules",
      "--to": "/compliance/modulesets/<moduleset>/modules",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.list_modules(options)
        self.add_module(options)
        self.remove_module(options)
        self.set_module(options)
        self.rename_module(options)

    def list_modules(self, options):
        if options.list is None or options.moduleset is None:
            return
        p = "/compliance/modulesets/%s/modules" % options.moduleset
        CmdLs(self.cli).cmd(p)

    def add_module(self, options):
        if options.add is None or options.moduleset is None or options.module is None:
            return
        global path
        _path = "/compliance/modulesets/%s/modules" % options.moduleset
        data = {
          "modset_mod_name": options.module,
        }
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def remove_module(self, options):
        if options.remove is None or options.moduleset is None or options.module is None:
            return
        global path
        _path = "/compliance/modulesets/%s/modules/%s" % (options.moduleset, options.module)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def rename_module(self, options):
        if options.rename is None or options.moduleset is None or options.module is None or options.to is None:
            return
        data = {
          "modset_mod_name": options.to,
        }
        _path = "/compliance/modulesets/%s/modules/%s" % (options.moduleset, options.module)
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)


    def set_module(self, options):
        if options.set is None or options.moduleset is None or options.module is None:
            return
        self.set_module_autofix(options)

    def set_module_autofix(self, options):
        if options.autofix is None:
            return
        data = {
          "autofix": options.autofix,
        }
        _path = "/compliance/modulesets/%s/modules/%s" % (options.moduleset, options.module)
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

class CmdVariable(Cmd):
    command = "variable"
    desc = "Handle design actions on a compliance variable"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--list", default=None, action="store_true", dest="list",
                      help="List variables in a ruleset")
    parser.add_option("--add", default=None, action="store_true", dest="add",
                      help="Add a variable to a ruleset")
    parser.add_option("--remove", default=None, action="store_true", dest="remove",
                      help="Remove a variable from a ruleset")
    parser.add_option("--copy", default=None, action="store_true", dest="copy",
                      help="Copy a variable to another ruleset")
    parser.add_option("--move", default=None, action="store_true", dest="move",
                      help="Move a variable to another ruleset")
    parser.add_option("--set", default=None, action="store_true", dest="set",
                      help="Set variable properties")
    parser.add_option("--rename", default=None, action="store_true", dest="rename",
                      help="Rename a variable")
    parser.add_option("--variable", default=None, action="store", dest="variable",
                      help="The name or id of the variable")
    parser.add_option("--ruleset", default=None, action="store", dest="ruleset",
                      help="The name or id of the variable's ruleset")
    parser.add_option("--to", default=None, action="store", dest="to",
                      help="With --rename, set the new variable name")
    parser.add_option("--dest-ruleset", default=None, action="store", dest="dest_ruleset",
                      help="With --copy or --move, set the name or id of the destination ruleset")
    parser.add_option("--class", default=None, action="store", dest="var_class",
                      help="With --set, set the variable class")
    parser.add_option("--value", default=None, action="store", dest="var_value",
                      help="With --set, set the variable value")
    parser.add_option("--value-edit", default=False, action="store_true", dest="var_value_edit",
                      help="With --set, spawn an editor on the variable expected data structure. Upon exit, the edited structure is saved as the variable value.")
    candidates_path = {
      "--ruleset": "/compliance/rulesets",
      "--dest-ruleset": "/compliance/rulesets",
      "--variable": "/compliance/rulesets/<ruleset>/variable",
      "--to": "/compliance/rulesets/<ruleset>/variable",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.list_variables(options)
        self.add_variable(options)
        self.remove_variable(options)
        self.set_variable(options)
        self.rename_variable(options)
        self.copy_variable(options)
        self.move_variable(options)

    def list_variables(self, options):
        if options.list is None or options.ruleset is None:
            return
        p = "/compliance/rulesets/%s/variables" % options.ruleset
        CmdLs(self.cli).cmd(p)

    def add_variable(self, options):
        if options.add is None or options.ruleset is None or options.variable is None:
            return
        global path
        _path = "/compliance/rulesets/%s/variables" % options.ruleset
        data = {
          "var_name": options.variable,
        }
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def remove_variable(self, options):
        if options.remove is None or options.ruleset is None or options.variable is None:
            return
        global path
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def rename_variable(self, options):
        if options.rename is None or options.ruleset is None or options.variable is None or options.to is None:
            return
        data = {
          "var_name": options.to,
        }
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def copy_variable(self, options):
        if options.copy is None or options.ruleset is None or options.variable is None or options.dest_ruleset is None:
            return
        data = {
          "action": "copy",
          "dst_ruleset": options.dest_ruleset,
        }
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.put(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def move_variable(self, options):
        if options.move is None or options.ruleset is None or options.variable is None or options.dest_ruleset is None:
            return
        data = {
          "action": "move",
          "dst_ruleset": options.dest_ruleset,
        }
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.put(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_variable(self, options):
        if options.set is None or options.ruleset is None or options.variable is None:
            return
        self.set_variable_class(options)
        self.set_variable_value(options)
        self.set_variable_value_edit(options)

    def set_variable_class(self, options):
        if options.var_class is None:
            return
        data = {
          "var_class": options.var_class,
        }
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_variable_value(self, options):
        if options.var_value is None:
            return
        data = {
          "var_value": options.var_value,
        }
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_variable_value_edit(self, options):
        if not options.var_value_edit:
            return

        # get variable class
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.get(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        variable = json.loads(bdecode(r.content))["data"][0]
        variable_class = str(variable["var_class"])
        variable_value = variable["var_value"]

        # get form definition
        _path = "/forms"
        params = {
          "query": "form_name="+variable_class,
        }
        r = requests.get(self.cli.api+_path, params=params, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        form = json.loads(bdecode(r.content))["data"][0]
        form_def = form["form_definition"]

        output_format = form_def["Outputs"][0]["Format"]

        # get current value
        if output_format != "raw":
            try:
                variable_data = json.loads(variable_value)
            except:
                variable_data = None
        else:
            variable_data = variable_value

        if variable_data is not None:
            pass
        elif output_format.endswith("dict"):
            d = {}
            for _input in form_def["Inputs"]:
                if "Key" in _input:
                    k = _input["Key"]
                else:
                    k = _input["Id"]
                v = "<%s. %s>" % (_input.get("Type", ""), _input.get("Help", ""))
                d[k] = v
        else:
            d = ""

        if variable_data is not None:
            text_data = variable_data
        elif output_format == "raw":
            text_data = ""
        elif output_format == "list":
            text_data = [d]
        elif output_format == "list of dict":
            text_data = [d]
        elif output_format == "dict of dict":
            text_data = {"<key>": d}
        elif output_format.startswith("dict"):
            text_data = d
        else:
            print("unknow format")
            return

        import tempfile
        f = tempfile.NamedTemporaryFile(prefix='variable_edit.')
        fname = f.name
        f.close()
        with open(fname, "w") as f:
            f.write(json.dumps(text_data, indent=4))

        os.system(find_editor()+" "+fname)
        with open(fname, "r") as f:
            buff = f.read()
        new_text_data = json.loads(buff)
        os.unlink(fname)
        if new_text_data == text_data:
            print("canceled (no change done in the editor)")
            return

        data = {
          "var_value": buff,
        }
        _path = "/compliance/rulesets/%s/variables/%s" % (options.ruleset, options.variable)
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)


class CmdRuleset(Cmd):
    command = "ruleset"
    desc = "Handle design actions on a compliance ruleset"
    parser = CmdOptionParser(description=desc)
    parser.add_option("--list", default=None, action="store_true", dest="list",
                      help="List rulesets")
    parser.add_option("--show", default=None, action="store_true", dest="show",
                      help="Show a ruleset design, with nesting")
    parser.add_option("--create", default=None, action="store_true", dest="create",
                      help="Create a ruleset")
    parser.add_option("--delete", default=None, action="store_true", dest="delete",
                      help="Delete a ruleset")
    parser.add_option("--set", default=None, action="store_true", dest="set",
                      help="Set a ruleset property")
    parser.add_option("--rename", default=None, action="store_true", dest="rename",
                      help="Rename a ruleset")
    parser.add_option("--attach", default=None, action="store_true", dest="attach",
                      help="Attach the ruleset to a filterset, a ruleset or a moduleset")
    parser.add_option("--detach", default=None, action="store_true", dest="detach",
                      help="Detach the ruleset from a filterset, a ruleset or a moduleset")
    parser.add_option("--clone", default=None, action="store_true", dest="clone",
                      help="Clone a ruleset, including variables, filterset and ruleset-ruleset relations. Reset the publication as responsible groups.")
    parser.add_option("--ruleset", default=None, action="store", dest="ruleset",
                      help="The name or id of the ruleset to manage")
    parser.add_option("--filterset", default=None, action="store", dest="filterset",
                      help="The name or id of the filterset to attach or detach")
    parser.add_option("--parent-ruleset", default=None, action="store", dest="parent_ruleset",
                      help="The name or id of the ruleset to attach to or detach from")
    parser.add_option("--parent-moduleset", default=None, action="store", dest="parent_moduleset",
                      help="The name or id of the moduleset to attach to or detach from")
    parser.add_option("--publication-group", default=None, action="store", dest="publication_group",
                      help="The name or id of the group to attach or detach as publication")
    parser.add_option("--responsible-group", default=None, action="store", dest="responsible_group",
                      help="The name or id of the group to attach or detach as responsible")
    parser.add_option("--public", default=None, action="store_true", dest="public",
                      help="With --set, set the public property to true")
    parser.add_option("--not-public", default=None, action="store_false", dest="public",
                      help="With --set, set the public property to false")
    parser.add_option("--contextual", default=None, action="store_true", dest="contextual",
                      help="With --set, set the type property to contextual")
    parser.add_option("--explicit", default=None, action="store_false", dest="explicit",
                      help="With --set, set the type property to explicit")
    parser.add_option("--to", default=None, action="store", dest="to",
                      help="with --rename, set the new ruleset name")
    candidates_path = {
      "--ruleset": "/compliance/rulesets",
      "--parent-moduleset": "/compliance/modulesets",
      "--parent-ruleset": "/compliance/rulesets",
      "--publication-group": "/groups",
      "--responsible-group": "/groups",
      "--filterset": "/filtersets",
    }
    api_candidates = False

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        self.clone_ruleset(options)
        self.list_rulesets(options)
        self.show_ruleset(options)
        self.create_ruleset(options)
        self.delete_ruleset(options)
        self.set_ruleset(options)
        self.rename_ruleset(options)
        self.attach_filterset_to_ruleset(options)
        self.detach_filterset_from_ruleset(options)
        self.attach_publication_group_to_ruleset(options)
        self.detach_publication_group_from_ruleset(options)
        self.attach_responsible_group_to_ruleset(options)
        self.detach_responsible_group_from_ruleset(options)
        self.attach_ruleset_to_ruleset(options)
        self.attach_ruleset_to_moduleset(options)
        self.detach_ruleset_from_ruleset(options)
        self.detach_ruleset_from_moduleset(options)

    def list_rulesets(self, options):
        if options.list is None:
            return
        p = "/compliance/rulesets"
        CmdLs(self.cli).cmd(p)

    def show_ruleset(self, options):
        if options.show is None:
            return
        o = CmdShow(self.cli)
        data = o.get_data("/compliance/rulesets/"+options.ruleset)
        o.print_ruleset(options.ruleset, data)

    def clone_ruleset(self, options):
        if options.clone is None or options.ruleset is None:
            return
        data = {
          "action": "clone",
        }
        _path = "/compliance/rulesets/%s" % options.ruleset
        r = requests.put(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def create_ruleset(self, options):
        if options.create is None or options.ruleset is None:
            return
        global path
        _path = "/compliance/rulesets"
        data = {
          "ruleset_name": options.ruleset,
        }
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def delete_ruleset(self, options):
        if options.delete is None or options.ruleset is None:
            return
        global path
        _path = "/compliance/rulesets/"+options.ruleset
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_ruleset(self, options):
        if options.set is None or options.ruleset is None:
            return
        self.set_ruleset_public(options)
        self.set_ruleset_type(options)

    def set_ruleset_public(self, options):
        if options.public is None:
            return
        data = {
          "ruleset_public": options.public,
        }
        _path = "/compliance/rulesets/%s" % options.ruleset
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def set_ruleset_type(self, options):
        if options.contextual is None and options.explicit is None:
            return
        if options.contextual is not None and options.explicit is not None:
            print("don't set both --explicit and --contextual")
            return
        if options.contextual:
            t = "contextual"
        if options.explicit:
            t = "explicit"
        data = {
          "ruleset_type": t,
        }
        _path = "/compliance/rulesets/%s" % options.ruleset
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_publication_group_to_ruleset(self, options):
        if options.attach is None or options.publication_group is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/publications/%s" % (options.ruleset, options.publication_group)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_publication_group_from_ruleset(self, options):
        if options.detach is None or options.publication_group is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/publications/%s" % (options.ruleset, options.publication_group)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_responsible_group_to_ruleset(self, options):
        if options.attach is None or options.responsible_group is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/responsibles/%s" % (options.ruleset, options.responsible_group)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_responsible_group_from_ruleset(self, options):
        if options.detach is None or options.responsible_group is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/responsibles/%s" % (options.ruleset, options.responsible_group)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_filterset_to_ruleset(self, options):
        if options.attach is None or options.filterset is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/filtersets/%s" % (options.ruleset, options.filterset)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_filterset_from_ruleset(self, options):
        if options.detach is None or options.filterset is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/filtersets/%s" % (options.ruleset, options.filterset)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_ruleset_to_ruleset(self, options):
        if options.attach is None or options.parent_ruleset is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/rulesets/%s" % (options.parent_ruleset, options.ruleset)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def attach_ruleset_to_moduleset(self, options):
        if options.attach is None or options.parent_moduleset is None or options.ruleset is None:
            return
        _path = "/compliance/modulesets/%s/rulesets/%s" % (options.parent_moduleset, options.ruleset)
        r = requests.post(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_ruleset_from_ruleset(self, options):
        if options.detach is None or options.parent_ruleset is None or options.ruleset is None:
            return
        _path = "/compliance/rulesets/%s/rulesets/%s" % (options.parent_ruleset, options.ruleset)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def detach_ruleset_from_moduleset(self, options):
        if options.detach is None or options.parent_moduleset is None or options.ruleset is None:
            return
        _path = "/compliance/modulesets/%s/rulesets/%s" % (options.parent_moduleset, options.ruleset)
        r = requests.delete(self.cli.api+_path, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)

    def rename_ruleset(self, options):
        if options.rename is None or options.ruleset is None or options.to is None:
            return
        data = {
          "ruleset_name": options.to,
        }
        _path = "/compliance/rulesets/%s" % options.ruleset
        r = requests.post(self.cli.api+_path, data=data, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        self.print_content(r.content)


class CmdShow(Cmd):
    api_candidates = True
    command = "show"
    desc = "Show a moduleset or a ruleset design and nesting."
    parser = CmdOptionParser(description=desc)

    def label(self, s):
        s = s.rstrip(":") + ":"
        if s in ("ruleset:", "type:", "public:", "filterset:", "stats:"):
            return self.colorize(s, c=self.GREEN)
        elif s in ("moduleset:"):
            return  self.colorize(s, c=self.DARKCYAN)
        elif s in ("publication group:", "responsible group:"):
            return  self.colorize(s, c=self.BLUE)
        elif s in ("variable:", "module:"):
            return  self.colorize(s, c=self.RED)
        return s

    def get_data(self, _path):
        r = requests.get(self.cli.api+_path+"/export", auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        data = json.loads(bdecode(r.content))

        # load hashes
        self.rulesets = {}
        for e in data.get("rulesets", []):
            self.rulesets[e.get("ruleset_name")] = e
        self.modulesets = {}
        for e in data.get("modulesets", []):
            self.modulesets[e.get("modset_name")] = e
        self.filtersets = {}
        for e in data.get("filtersets", []):
            self.modulesets[e.get("fset_name")] = e

        return data

    def cmd(self, line):
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        _path = self.args_to_path(args)
        data = self.get_data(_path)
        _path = self.factorize_dot_dot(_path)
        obj_type = _path.split("/")[-2]
        obj_id = _path.split("/")[-1]
        try:
            obj_id = int(obj_id)
        except:
            pass

        if obj_type == "modulesets":
            self.print_moduleset(obj_id, data)
        elif obj_type == "rulesets":
            self.print_ruleset(obj_id, data)
        elif obj_type == "filtersets":
            self.print_filterset(obj_id, data)
        else:
            print("unsupported object type:", obj_type)

    def print_moduleset(self, obj_id, data):
        for e in data.get("modulesets", []):
            if e.get("modset_name") == obj_id or e.get("id") == obj_id:
                self.print_export_moduleset(e, data)

    def print_ruleset(self, obj_id, data):
        for e in data.get("rulesets", []):
            if e.get("ruleset_name") == obj_id or e.get("id") == obj_id:
                self.print_export_ruleset(e, data)

    def print_filterset(self, obj_id, data):
        for e in data.get("filtersets", []):
            if e.get("fset_name") == obj_id or e.get("id") == obj_id:
                self.print_export_filterset(e, data)

    def iprint(self, *args, **vars):
        lvl = vars.get("lvl", 0)
        if lvl > 0:
            args = ["    "*lvl] + list(args)
        print(*args)

    def print_export_rulesets(self, data, lvl=0):
        for e in data["rulesets"]:
            self.print_export_ruleset(e, data, lvl=lvl)

    def print_export_modulesets(self, data, lvl=0):
        for e in data["modulesets"]:
            self.print_export_moduleset(e, data, lvl=lvl)

    def print_export_filtersets(self, data, lvl=0):
        for e in data["filtersets"]:
            self.print_export_filterset(e, data, lvl=lvl)

    def print_export_ruleset(self, rset, data, lvl=0):
        self.iprint(self.label("ruleset"), rset.get("ruleset_name"), lvl=lvl)
        self.iprint(self.label("public"), rset.get("ruleset_public"), lvl=lvl+1)
        self.iprint(self.label("type"), rset.get("ruleset_type"), lvl=lvl+1)
        if rset.get("fset_name"):
            self.iprint(self.label("filterset"), rset.get("fset_name"), lvl=lvl+1)
        for e in rset.get("publications"):
            self.iprint(self.label("publication group"), e, lvl=lvl+1)
        for e in rset.get("responsibles"):
            self.iprint(self.label("responsible group"), e, lvl=lvl+1)
        for e in rset.get("variables"):
            self.iprint(self.label("variable"), e.get("var_class"), e.get("var_name"), lvl=lvl+1)
        for e in rset.get("rulesets"):
            _e = self.rulesets.get(e)
            if _e is None:
                continue
            self.print_export_ruleset(_e, data, lvl=lvl+1)

    def print_export_moduleset(self, modset, data, lvl=0):
        self.iprint(self.label("moduleset"), modset.get("modset_name"), lvl=lvl)
        for e in modset.get("publications"):
            self.iprint(self.label("publication group"), e, lvl=lvl+1)
        for e in modset.get("responsibles"):
            self.iprint(self.label("responsible group"), e, lvl=lvl+1)
        for e in modset.get("modules"):
            autofix = e.get("autofix")
            if autofix:
                autofix = "(autofix)"
            else:
                autofix = ""
            self.iprint(self.label("module"), e.get("modset_mod_name"), autofix, lvl=lvl+1)
        for e in modset.get("rulesets"):
            _e = self.rulesets.get(e)
            if _e is None:
                continue
            self.print_export_ruleset(_e, data, lvl=lvl+1)
        for e in modset.get("modulesets"):
            _e = self.modulesets.get(e)
            if _e is None:
                continue
            self.print_export_moduleset(_e, data, lvl=lvl+1)

    def print_export_filterset(self, rset, data, lvl=0):
        self.iprint(self.label("filterset"), rset.get("fset_name"), lvl=lvl)
        self.iprint(self.label("stats"), rset.get("fset_stats"), lvl=lvl+1)
        for e in rset.get("filters"):
            if e.get("filterset"):
                self.iprint(self.colorize(str(e.get("f_order"))+":", c=self.RED),
                            e.get("f_log_op"),
                            e.get("filterset"),
                            lvl=lvl+1)
            else:
                f = e.get("filter")
                self.iprint(self.colorize(str(e.get("f_order"))+":", c=self.RED),
                            e.get("f_log_op"),
                            f.get("f_table")+"."+f.get("f_field"), f.get("f_op"), f.get("f_value"),
                            lvl=lvl+1)


def validate_response(r):
    if r.status_code == 200:
        return
    try:
        data = json.loads(bdecode(r.content))
        raise CliError("%d %s" % (r.status_code, data["error"]))
    except (ValueError, KeyError):
        pass
    raise CliError(str(r))

class CmdGet(Cmd):
    api_candidates = True
    command = "get"
    desc = "Execute a GET request on the given API handler. The parameters can be set using --<param>."
    parser = CmdOptionParser(description=desc)

    def cmd(self, line):
        self.set_parser_options_from_cmdline(line)
        global path
        try:
            options, args = self.parser.parse_args(args=shlex.split(line))
        except Exception as e:
            try: print(e)
            except: pass
            return
        params = options.__dict__
        _path = self.args_to_path(args)
        r = requests.get(self.cli.api+_path, params=params, auth=self.cli.auth, verify=not self.cli.insecure)
        validate_response(r)
        try:
            # try not to display \u0000 in the output
            d = json.loads(bdecode(r.content))
            self.print_content(json.dumps(d, ensure_ascii=False, indent=8))
        except Exception as e:
            self.print_content(r.content)

class CmdHistory(Cmd):
    command = "history"
    desc = "Display the commands history"
    parser = CmdOptionParser(description=desc)
    max_lines = 200

    def candidates(self, p):
        return []

    def cmd(self, line):
        n = readline.get_current_history_length()
        if n > self.max_lines:
            m = self.max_lines
        else:
            m = n
        print("n", n)
        print("m", m)
        for i in range(n-m, n):
            print("%-6d %s" % (i, readline.get_history_item(i)))

class CmdCd(Cmd):
    api_candidates = True
    command = "cd"
    desc = "Change the current working directory in the API handlers tree."
    parser = CmdOptionParser(description=desc)
    prev_paths = ["/"]
    max_prev_paths = 10

    def append_to_prev_paths(self, p):
        global path
        if path == self.prev_paths[-1]:
            return
        self.prev_paths.append(copy.copy(path))
        if len(self.prev_paths) <= self.max_prev_paths:
            return
        for i in range(len(self.prev_paths)-self.max_prev_paths):
            dump = self.prev_paths.pop(0)

    def set_new_path(self, p):
        global path
        self.append_to_prev_paths(p)
        path = p

    def cmd(self, line):
        global path
        m = re.match(r"^cd\s+(?P<path>[% @\-\./\w]+)$", line)
        if m is None:
            return
        p = m.group("path")

        # handle "cd -"
        if p == "-":
            new_path = self.prev_paths.pop()
            self.set_new_path(new_path)
            return

        if p != "/":
            p = p.rstrip("/")

        l = path.split("/")
        v = p.split("/")
        for elem in copy.copy(v):
            if elem != "..":
                break
            l.pop()
            v.pop(0)
        new_path = "/".join(l)
        if new_path == "":
            new_path = "/"
        p = "/".join(v)
        if p == "":
            self.set_new_path(new_path)
            return

        if p.startswith("/"):
            new_path = p
        else:
            new_path += "/" + p
        new_path = new_path.replace("//", "/")
        if self.path_match_handlers_or_parents(new_path):
            self.set_new_path(new_path)
            return
        print("path not found")
        return

def path_match_handler(p, d):
    if p == "/":
        return True
    p = p.rstrip("/")
    pattern = d["pattern"]
    if re.match(pattern, p) is not None:
        return True
    return False

def path_match_handler_or_parents(p, d):
    if p == "/":
        return True
    pattern = d["pattern"]
    if re.match(pattern, p) is not None:
        return True
    for i in range(pattern.count("/")):
        pattern = pattern[:pattern.rindex("/")]
        pattern2 = pattern + "[/]*$"
        if re.match(pattern2, p) is not None:
            return True
    return False

class Completer(object):

    def __init__(self, commands):
        self.commands = commands
        self.current_candidates = []
        self.commands_h = {}
        for c in commands:
            self.commands_h[c.command] = c
        self.base_commands = self.commands_h.keys()

    def complete(self, text, state):
        response = None
        if state == 0:
            # This is the first time for this text, so build a match list.

            origline = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = origline[begin:end]
            words = origline.split()

            #print('origline=%s'% repr(origline))
            #print('begin=%s'% begin)
            #print('end=%s'% end)
            #print('being_completed=%s'% being_completed)
            #print('words=%s'% words)

            self.current_candidates = sorted(self.base_commands)
            try:
                if begin == 0:
                    # first word
                    candidates = self.current_candidates
                else:
                    # later word
                    command = " ".join(words)
                    while (command != ""):
                        if command in self.commands_h:
                            c = self.commands_h[command]
                            break
                        command = " ".join(command.split()[:-1])
                    if command == "":
                        raise KeyError("command not supported")
                    candidates = []
                    candidates += c.candidates(being_completed, words)
                if being_completed:
                    # match options with portion of input
                    # being completed
                    self.current_candidates = [ w for w in candidates
                                                if w.startswith(being_completed) ]
                else:
                    # matching empty string so use all candidates
                    self.current_candidates = candidates

                #print('candidates=%s', self.current_candidates)

            except (KeyError, IndexError) as err:
                self.current_candidates = []

        try:
            response = self.current_candidates[state]
        except IndexError:
            response = None
        #print('complete(%s, %s) => %s'% (repr(text), state, response))
        return response


class Cli(object):
    def __init__(self, user=None, password=None, api=None, insecure=None, refresh_api=None, fmt=None, config=conf_f, save=True):
        self.options = Storage({
            "user": user,
            "password": password,
            "api": api,
            "refresh_api": refresh_api,
            "insecure": insecure,
            "config": config,
            "format": fmt,
            "save": save,
        })

        self.read_config()
        self.parse_options()
        self.do_refresh_api()

        self.commands = [
          CmdCd(cli=self),
          CmdLs(cli=self),
          CmdHistory(cli=self),
          CmdGet(cli=self),
          CmdPost(cli=self),
          CmdPut(cli=self),
          CmdDelete(cli=self),
          CmdShow(cli=self),
          CmdHelp(cli=self),
          CmdRuleset(cli=self),
          CmdVariable(cli=self),
          CmdModuleset(cli=self),
          CmdModule(cli=self),
          CmdFilter(cli=self),
          CmdFilterset(cli=self),
          CmdSysreport(cli=self),
          CmdSafe(cli=self),
        ]


    def dispatch(self, line):
        if line.strip() == "":
            return
        for command in self.commands:
            if not command.match(line):
                continue
            try:
                return command.cmd(line)
            except CliError as e:
                print(str(e), file=sys.stderr)
                return 1
        print("command not found:", line)

    def parse_options(self):
        self.need_save = False
        self.user = self.set_option("user")
        self.password = self.set_option("password")
        self.api = self.set_option("api")
        self.format = self.set_option("format", "json")
        self.insecure = self.set_option("insecure", False)
        self.auth = (self.user, self.password)

        if self.insecure and InsecureRequestWarning is not None:
            try:
                requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
            except AttributeError:
                pass

        self.host = self.api.replace("https://", "").replace("http://", "")
        if "/" in self.host:
            self.host = self.host[:self.host.index("/")]

        if not self.api.endswith("/rest/api"):
            self.api = "https://" + self.host + "/init/rest/api"

        if self.need_save and self.options.save:
            self.save_config()

    def save_config(self):
        """ Save options if no config file is present yet.
        """
        if self.user is None or self.password is None or self.api is None or self.insecure is None:
            return
        print("updating %s config file with provided parameters" % self.options.config, file=sys.stderr)
        if not self.conf.has_section(conf_section):
            self.conf.add_section(conf_section)
        self.conf.set(conf_section, "user", self.user)
        self.conf.set(conf_section, "password", self.password)
        self.conf.set(conf_section, "api", self.api)
        self.conf.set(conf_section, "insecure", self.insecure)
        self.conf.set(conf_section, "format", self.format)
        with open(self.options.config, 'w') as fp:
            self.conf.write(fp)
        os.chmod(self.options.config, 0o0600)

    def do_refresh_api(self):
        try:
            self.api_o = Api(cli=self, refresh=self.options.refresh_api)
        except Exception as e:
            raise ex.Error(str(e))

    def read_config(self):
        if os.path.exists(self.options.config):
            s = os.stat(self.options.config)
            if s.st_mode & stat.S_IWOTH:
                print("set ", self.options.config, "mode to 0600")
                os.chmod(self.options.config, 0o0600)

        try:
            self.conf = ConfigParser.RawConfigParser()
            self.conf.read(self.options.config)
        except:
            pass

    def set_option(self, o, default=None):
        if self.options[o] == "?":
            self.need_save = True
            if o == "password":
                import getpass
                return getpass.getpass()
            else:
                return input(o+": ")
        if self.options[o] is not None:
            self.need_save = True
            return self.options[o]
        if self.conf.has_option(conf_section, o):
            return self.conf.get(conf_section, o)
        if default is not None:
            return default
        raise ex.Error("missing parameter: "+o)

    def dispatch_noninteractive(self, args):
        # non interactive mode
        import subprocess
        line = subprocess.list2cmdline(args)
        try:
            return self.dispatch(line)
        except Exception as exc:
            raise ex.Error(str(exc))

    def readline_setup(self):
        readline.parse_and_bind('tab: complete')
        atexit.register(readline.write_history_file, history_f)
        try:
            readline.read_history_file(history_f)
        except IOError:
            pass
        readline.set_completer(Completer(self.commands).complete)
        delims = readline.get_completer_delims()
        delims = delims.replace("-", "").replace("/", "").replace("@", "").replace("%", "")
        readline.set_completer_delims(delims)

    def input_loop(self):
        self.readline_setup()
        line = ''
        while line not in ('exit', 'quit'):
            try:
                line = input(self.host+":"+path+' # ')
                self.dispatch(line)
            except ValueError as exc:
                print(exc)
                readline.redisplay()
                pass
            except EOFError:
                print()
                return
            except KeyboardInterrupt:
                print()
                readline.redisplay()
                pass
            except Exception as e:
                import traceback
                e = sys.exc_info()
                print(e[0], e[1], traceback.print_tb(e[2]))


    def run(self, argv=None):
        if argv and len(argv) > 0:
            return self.dispatch_noninteractive(argv)
        else:
            self.input_loop()

if __name__ == "__main__":
    cli = Cli()
    ret = cli.run()
    sys.exit(ret)

