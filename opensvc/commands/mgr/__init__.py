# coding: utf8

"""
This executable is wrapped by the opensvc shell script.

It's the entrypoint for all OpenSVC services management ops.
"""
from __future__ import print_function
from __future__ import absolute_import

import importlib
import os
import sys

import core.status
import core.exceptions as ex
import utilities.render.color
from core.node import Node
from env import Env
from utilities.naming import split_path, validate_kind
from utilities.proc import check_privs, get_option
from utilities.storage import Storage


class Mgr(object):
    def __init__(self, parser=None, node=None, selector=None):
        self.parser = parser
        self.node = node
        self.selector = selector
        self.expanded_svcs = None

    @staticmethod
    def get_extra_argv(argv=None):
        """
        Extract oci/docker/podman/... passed-through argv from the command main argv.

        The service management command acts as a wrapper for podman and docker commands,
        setting the service-specific socket if necessary.
        """
        commands = ["oci", "podman", "docker"]
        if argv is None:
            argv = sys.argv[1:]
        if len(argv) < 2:
            return argv, []
        for command in commands:
            try:
                pos = argv.index(command)
                extra_argv = argv[pos + 1:]
                argv = argv[:pos + 1]
                return argv, extra_argv
            except Exception:
                extra_argv = []
        return argv, []

    @staticmethod
    def get_build_kwargs(options, action):
        """
        Return the service build function keyword arguments, deduced from
        parsed command options.
        """
        build_kwargs = {}

        if len(set(["paths", "status"]) & set(build_kwargs.keys())) == 0:
            if hasattr(options, "svcs") and options.svcs is not None:
                build_kwargs["paths"] = options.svcs

        if hasattr(options, "status") and options.status is not None:
            build_kwargs["status"] = [core.status.status_value(s) for s in options.status.split(",")]

        build_kwargs["create_instance"] = action in ("create", "pull")

        return build_kwargs

    def do_svcs_action_detached(self, argv=None):
        """
        Executes the services action in detached process mode, so that
        a term/kill signal on the parent process does not abort the action.

        Keyboard interrupts do abort the detached process though.
        """
        ret = 0
        env = {}
        env.update(os.environ)
        env["OSVC_DETACHED"] = "1"
        env["OSVC_PARENT_SESSION_UUID"] = Env.session_uuid
        try:
            import subprocess
            import signal
            kwargs = {}
            try:
                kwargs["preexec_fn"] = os.setsid
            except AttributeError:
                pass
            prog = os.path.dirname(os.path.abspath(__file__))
            prog = os.path.join(prog, "..", "..", "__main__.py")
            prog = os.path.realpath(prog)
            if self.selector:
                executable = [sys.executable, prog, self.selector]
            elif os.environ.get("OSVC_KIND"):
                executable = [sys.executable, prog, os.environ.get("OSVC_KIND")]
            else:
                executable = [sys.executable, prog]

            proc = subprocess.Popen(executable + argv,
                                    stdout=None, stderr=None, stdin=None,
                                    close_fds=True, cwd=os.sep,
                                    env=env, **kwargs)
            proc.wait()
            ret = proc.returncode
        except KeyboardInterrupt as exc:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            print("kill detached process")
            ret = 1
        except ex.Signal as exc:
            print("the action, detached as pid %d, "
                  "will continue executing" % proc.pid)
            ret = 1
        except Exception as exc:
            print(exc, file=sys.stderr)
            ret = 1
        return ret

    def do_svcs_action(self, options, action, argv):
        """
        Execute the services action, switching between detached mode for
        stop*/shutdown/unprovision/switch, and inline mode for other actions.
        """
        ret = 0

        if os.environ.get("OSVC_ACTION_ORIGIN") != "daemon" and \
                os.environ.get("OSVC_DETACHED") != "1" and \
                (action in ("stop", "shutdown", "unprovision") or
                 (action == "delete" and options.unprovision is True)):
            ret = self.do_svcs_action_detached(argv)
        else:
            try:
                ret = self.node.do_svcs_action(action, options)
            except ex.Error as exc:
                print(exc, file=sys.stderr)
                ret = 1
        return ret

    def prepare_options(self, options):
        """
        Prepare and return the options Storage() as expected by the Svc::action
        and Node::do_svcs_action methods.
        """
        opts = Storage()
        # preserve parm_svcs, as svcs will be expanded
        opts.parm_svcs = options.parm_svcs
        for key, val in options.__dict__.items():
            opts[key.replace("parm_", "")] = val
        try:
            namespace = options.namespace
        except AttributeError:
            namespace = None
        if namespace:
            opts.namespace = namespace
        elif "OSVC_NAMESPACE" in os.environ:
            opts.namespace = os.environ["OSVC_NAMESPACE"]
        if self.selector:
            opts.parm_svcs = self.selector
            opts.svcs = self.selector
        if opts.eval and not opts.format:
            opts.format = "json"
        return opts

    @staticmethod
    def split_env(arg):
        idx = arg.index("=")
        option = arg[:idx]
        value = arg[idx + 1:]
        return option, value

    def export_env_from_options(self, options):
        if options.get("daemon"):
            os.environ["OSVC_DETACHED"] = "1"
        for arg in options.get("env", []):
            if arg in ("-", "stdin", "/dev/stdin"):
                continue
            option, value = self.split_env(arg)
            option = option.upper()
            os.environ[option] = value

    @staticmethod
    def dispatch_svcs(paths):
        data = {}
        for path in paths:
            try:
                _, _, kind = split_path(path)
            except ValueError:
                continue
            try:
                validate_kind(kind)
            except ValueError as exc:
                raise ex.Error(str(exc))
            try:
                data[kind].append(path)
            except KeyError:
                data[kind] = [path]
        return data

    def get_action(self, argv):
        action = []
        _in = False
        for word in argv:
            if _in and word.startswith("-"):
                break
            if word.startswith("-"):
                continue
            action.append(word)
            _in = True
        return "_".join(action)

    def dispatch(self, argv):
        if self.selector is None:
            yield
        namespace = get_option("--namespace", argv)
        if namespace is None:
            namespace = os.environ.get("OSVC_NAMESPACE")
        action = self.get_action(argv)
        if action in ("create", "deploy"):
            expanded_svcs = self.selector.split(",")
        elif action == "ls":
            from commands.svc.parser import SvcOptParser
            parser = SvcOptParser()
            expanded_svcs = None
            yield parser
        else:
            local = action == "boot"
            expanded_svcs = self.node.svcs_selector(self.selector, namespace, local=local)
            self.expanded_svcs = expanded_svcs
        if expanded_svcs is not None:
            svc_by_kind = self.dispatch_svcs(expanded_svcs)
            for kind, paths in svc_by_kind.items():
                modname = "commands.{kind}.parser".format(kind=kind)
                mod = importlib.import_module(modname)
                parser = getattr(mod, kind.capitalize() + "OptParser")()
                yield parser

    def parse_args(self, argv):
        if self.parser:
            self.optparser = self.parser()
            return self.optparser.parse_args(argv)
        err = []
        for parser in self.dispatch(argv):
            if not parser:
                continue
            self.optparser = parser
            try:
                options, action = self.optparser.parse_args(argv)
                return options, action
            except Exception as exc:
                # if any parser accepts this argv, don't display errors
                # raised by other parsers. keep them around in case no
                # parser accepts.
                buff = str(exc)
                if not buff.startswith(self.optparser.prog):
                    buff = "%s: %s" % (self.optparser.prog, buff)
                err.append(buff)
                pass
        # no parser matched. display a per-parser errorlog
        raise ex.Error("\n".join(err))

    def _main(self, argv=None):
        """
        Build the service list.
        Execute action-specific codepaths.
        """
        build_err = False
        ret = 0

        argv, extra_argv = self.get_extra_argv(argv)
        try:
            options, action = self.parse_args(argv)
        except ex.Error as exc:
            if str(exc):
                raise
            else:
                raise ex.Error("no match")
        if action == "deploy":
            action = "create"
            options.provision = True
        options = self.prepare_options(options)
        self.export_env_from_options(options)
        options.extra_argv = extra_argv
        utilities.render.color.use_color = options.color
        try:
            self.node.options.format = options.format
            self.node.options.jsonpath_filter = options.jsonpath_filter
        except AttributeError:
            pass
        if action not in ("ls", "monitor", "create") and options.svcs is None and options.status is None:
            raise ex.Error("no object selected.")
        if action in ("ls", "monitor") and options.svcs is None:
            kind = os.environ.get("OSVC_KIND", "svc")
            options.svcs = "*/%s/*" % kind

        if action == "create":
            if options.svcs:
                options.svcs = options.svcs.split(",")
        else:
            if self.expanded_svcs is None:
                local = action == "boot"
                expanded_svcs = self.node.svcs_selector(options.svcs, options.namespace, local=local)
            else:
                expanded_svcs = self.expanded_svcs
            if options.svcs in (None, "*") and expanded_svcs == []:
                return
            options.svcs = expanded_svcs

        self.node.set_rlimit()
        build_kwargs = self.get_build_kwargs(options, action)

        if action != "create":
            try:
                self.node.build_services(**build_kwargs)
            except ex.Error as exc:
                if len(str(exc)) > 0:
                    print(exc, file=sys.stderr)
                build_err = True

        if action not in ("ls", "create") and len(options.svcs) == 0:
            if not build_err:
                sys.stderr.write("no match\n")
            return 1

        if action == "create":
            return self.node.create_service(options.svcs, options)

        ret = self.do_svcs_action(options, action, argv=argv)

        try:
            import logging
            logging.shutdown()
        except:
            pass

        return ret

    def __call__(self, argv=None):
        """
        Instanciate a Node object.
        Call the real deal making sure the node is finally freed.
        """
        ret = 0

        if self.node is None:
            try:
                self.node = Node()
            except Exception as exc:
                print(exc, file=sys.stderr)
                return 1

        check_privs()

        try:
            ret = self._main(argv=argv)
        except ex.Error as exc:
            print(exc, file=sys.stderr)
            return 1
        except ex.Version as exc:
            print(exc)
            return 0
        except KeyboardInterrupt:
            return 1
        finally:
            self.node.close()

        if ret is None:
            ret = 0

        return ret


if __name__ == "__main__":
    selector = sys.argv[1]
    del sys.argv[1]
    sys.exit(Mgr(selector=selector)())
