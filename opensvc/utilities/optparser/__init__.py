"""
Helper module to handle optparser configuration.

Define a reference of supported keywords, their supported options, and methods
to format contextualized help messages.
"""

from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import optparse
import textwrap
import utilities.render.color
import core.exceptions as ex
import re

from utilities.render.term import term_width
from utilities.string import is_string
from utilities.version import agent_version


def wipe_rest_markup(payload):
    payload = re.sub(r':(cmd|kw|opt|c-.*?):`(.*?)`', lambda pat: "'" + pat.group(2) + "'", payload, re.MULTILINE)
    payload = re.sub(r'``(.*?)``', lambda pat: "'" + pat.group(1) + "'", payload, re.MULTILINE)
    return payload


class Option(optparse.Option):
    pass


class OsvcHelpFormatter(optparse.TitledHelpFormatter):
    def format_option(self, option):
        if option in self.deprecated_options:
            return ""
        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:                       # start help on same line as opts
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option)
            help_lines = []
            for block in help_text.splitlines():
                help_lines += textwrap.wrap(block, self.help_width)
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line)
                           for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        result.append("\n")
        return "".join(result).replace("``", "`")


class OptionParserNoHelpOptions(optparse.OptionParser):
    deprecated_options = []

    def format_help(self, formatter=None):
        if formatter is None:
            formatter = self.formatter
        result = []
        if self.usage:
            result.append(self.get_usage() + "\n")
        if self.description:
            result.append(self.format_description(formatter) + "\n")
        result.append(self.format_epilog(formatter))
        return "".join(result)

    def exit(self, status=0, msg=None):
        """
        Override optparse.exit so sys.exit doesn't get called.
        """
        raise ex.Error(msg)

    def error(self, msg):
        """
        Override optparse.error so sys.exit doesn't get called.
        """
        raise ex.Error(msg)

    def print_version(self, file=None):
        """
        Override optparse.error so sys.exit doesn't get called.
        """
        if self.version:
            msg = self.get_version()
        else:
            msg = ""
        raise ex.Version(msg)


class OptParser(object):
    """
    A class wrapping the optparse module use, adding some features:
    * contextualized help depending on action prefix
    * colors
    * layout tweaks
    """

    def __init__(self, args=None, prog="", options=None, actions=None,
                 deprecated_options=None,
                 deprecated_actions=None, actions_translations=None,
                 global_options=None, svc_select_options=None, colorize=True,
                 width=None, formatter=None, indent=6, async_actions=None):
        self.parser = None
        self.args = args
        self.prog = prog
        self.version = prog + " version undef"
        self.options = options
        self.actions = actions
        self.deprecated_options = [self.options[name] for name in deprecated_options] if deprecated_options else []
        self.deprecated_actions = deprecated_actions if deprecated_actions else []
        self.actions_translations = actions_translations if actions_translations else {}
        self.global_options = global_options if global_options else []
        self.svc_select_options = svc_select_options if svc_select_options else []
        self.colorize = colorize
        self.width = term_width() if width is None else width

        self.usage = self.prog + " [ OPTIONS ] COMMAND\n\n"
        self.indent = indent
        self.subsequent_indent = " " * self.indent
        if formatter is None:
            self.formatter = OsvcHelpFormatter(self.indent,
                                               self.indent+2,
                                               self.width)
        else:
            self.formatter = formatter
        self.formatter.deprecated_options = self.deprecated_options
        if async_actions is None:
            self.async_actions = {}
        else:
            self.async_actions = async_actions
        self.formatter.format_heading = lambda x: "\n"
        self.get_parser()

    def get_valid_actions(self, section, action):
        """
        Given a section and an action prefix, return the list of
        valid actions
        """
        valid_actions = []
        for candidate_action in sorted(self.actions[section]):
            if is_string(action) and \
               not candidate_action.startswith(action):
                continue
            if isinstance(action, list) and candidate_action not in action:
                continue
            valid_actions.append(candidate_action)
        return valid_actions

    def format_options(self, section, action):
        """
        Format the possible options for a spectific action.
        """
        desc = ""
        parser = OptionParserNoHelpOptions(formatter=self.formatter, add_help_option=False)
        for option in self.actions[section][action].get("options", []):
            parser.add_option(option)
        for option in self.global_options:
            parser.add_option(option)
        desc += self.subsequent_indent + parser.format_option_help()
        return desc

    def format_action(self, section, action, options=True):
        """
        Format an candidate action for the help message.
        The action message may or may include the possible options,
        dependendin on the value of the options parameter.
        """
        fancya = self.prog + " " + action.replace('_', ' ')
        if self.colorize:
            desc = "  " + utilities.render.color.colorize(fancya, utilities.render.color.color.BOLD)
        else:
            desc = "  " + fancya
        desc += '\n\n'
        if self.async_actions.get(action, {}).get("local"):
            preamble = "Asynchronous orchestrated action, unless --local or --node <node> is specified.\n\n"
        else:
            preamble = ""
        wrapper = textwrap.TextWrapper(width=self.width-self.indent, replace_whitespace=False)
        text = preamble + self.actions[section][action]["msg"]
        text = text.replace("``", "`")
        for phrase in text.splitlines():
            for line in wrapper.wrap(phrase):
                for _line in line.splitlines():
                    desc += self.subsequent_indent+_line
            desc += '\n'

        if options:
            desc += self.format_options(section, action)

        desc = wipe_rest_markup(desc)
        desc += '\n'
        return desc

    def format_digest(self, action=""):
        """
        Format and return a digest of supported actions matching <action>
        """
        action = action.rstrip("?")
        desc = self.usage
        desc = desc.replace("COMMAND", action + "...")
        desc = desc.replace("[ OPTIONS ] ", "")
        desc += "  --help   display action description and supported options.\n\n"
        desc += self.format_valid_actions(action)
        return desc

    def format_valid_actions(self, action):
        desc = ""
        for section in sorted(self.actions):
            valid_actions = self.get_valid_actions(section, action)
            if len(valid_actions) == 0:
                continue

            desc += section + "\n\n"

            for valid_action in valid_actions:
                desc += "  " + valid_action.replace("_", " ") + "\n"
            desc += "\n"
        if desc:
            return desc[:-2]
        return self.format_valid_actions("")

    def format_desc(self, svc=False, action=None, options=True):
        """
        Format and return the help message, contextualized to display
        only actions matching the action argument.
        """
        desc = self.usage
        for section in sorted(self.actions):
            valid_actions = self.get_valid_actions(section, action)
            if len(valid_actions) == 0:
                continue

            desc += section + '\n'
            desc += '-' * len(section)
            desc += "\n\n"

            for valid_action in valid_actions:
                if svc and not hasattr(svc, valid_action):
                    continue
                try:
                    desc += self.format_action(section, valid_action, options=options)
                except ValueError:
                    # http://bugs.python.org/issue13107 triggered by lxc-attach
                    # term environment.
                    desc += action + "\n"
        return desc[0:-2]

    def supported_actions(self):
        """
        Return the list of actions supported by the command.
        """
        actions = []
        for section in self.actions:
            actions += self.actions[section].keys()
        actions += self.deprecated_actions
        return actions

    def actions_next_words(self, base=""):
        """
        From actions ["do_this", "do_that", "or_do_that"]:
        base="do"    => set([this, that, do])
        base="or_do" => set([that])
        """
        data = set()
        if base == "":
            prefix = base
        else:
            prefix = base + "_"
        prefix_len = len(prefix)
        for section in self.actions:
            for action in self.actions[section]:
                if base != "" and not action.startswith(base+'_'):
                    continue
                words = action[prefix_len:].split("_")
                if len(words) == 0:
                    continue
                data.add(words[0])
        return data

    def actions_all(self):
        data = []
        for section in self.actions:
            data += list(self.actions[section].keys())
        return data

    def develop_action(self, args):
        """
        From "ed conf" return "edit_config"
        """
        developed_args = []
        for idx, arg in enumerate(args):
            data = self.actions_next_words("_".join(developed_args))
            if arg in data:
                developed_args.append(arg)
                continue
            matching = [word for word in data if word.startswith(arg)]
            if len(matching) == 1:
                developed_args.append(matching[0])
                continue
            elif len(matching) == 0 and idx > 0:
                developed_args = []
                break
            else:
                # ambiguous
                return "_".join(developed_args+args[idx:])
        developed_action = "_".join(developed_args)
        if developed_action in self.actions_all():
            return developed_action
        while True:
            data = self.actions_next_words("_".join(developed_args))
            if len(data) == 0:
                break
            if len(data) == 1:
                developed_args.append(list(data)[0])
                continue
            break
        return "_".join(developed_args)

    def get_action_from_args(self, args, options):
        """
        Check if the parsed command args list has at least one element to be
        interpreted as an action. Raise if not, else return the action name
        formatted as a '_' joined string.
        """
        if len(args) == 0:
            if options.parm_help:
                self.print_full_help()
            else:
                self.print_short_help()

        action = self.develop_action(args)

        if action in self.actions_translations:
            data = self.actions_translations[action]
            if isinstance(data, dict):
                action = data["action"]
                options = data["mangle"](options)
            else:
                action = data

        return action, options

    def get_parser(self):
        """
        Setup an optparse parser
        """
        if self.parser is not None:
            return

        try:
            from version import version
        except ImportError:
            try:
                version = agent_version()
            except IndexError:
                version = "dev"

        self.version = self.prog + " version " + version

        self.parser = OptionParserNoHelpOptions(
            version=self.version,
            add_help_option=False,
        )

        for option in self.options.values():
            self.parser.add_option(option)

    def set_full_usage(self):
        """
        Setup for display of all actions and options.
        Used by the man page generator.
        """
        usage = self.format_desc(action=None, options=True)
        self.parser.set_usage(usage)

    def parse_args(self, argv=None):
        """
        Parse system's argv, validate options compatibility with the action
        and return options and action
        """
        if argv is not None:
            self.args = argv
        else:
            self.args = sys.argv[1:]

        # parse a first time with all possible options to never fail on
        # undefined option.
        options, args = self.parser.parse_args(self.args)
        action, options = self.get_action_from_args(args, options)

        # now we know the action. and we know if --help was set.
        # we can prepare a contextualized usage message.
        if options.parm_help and not action:
            self.print_digest()

        usage = self.format_desc(action=action, options=options.parm_help)
        self.parser.set_usage(usage)

        if options.parm_help or action not in self.supported_actions():
            self.print_context_help(action, options)

        # parse a second time with only options supported by the action
        # so we can raise on options incompatible with the action
        parser = OptionParserNoHelpOptions(
            version=self.version,
            usage=usage,
            add_help_option=False,
        )

        action_options = []
        for section_data in self.actions.values():
            if action not in section_data:
                continue
            action_options = section_data[action].get("options", [])
        for option in action_options + self.global_options:
            try:
                parser.add_option(option)
            except TypeError as exc:
                raise ex.Error("misclassified option: %s" % exc)
        options_discarded, args_discarded = parser.parse_args(self.args, optparse.Values())
        return options, action


    def print_digest(self):
        """
        Reset the parser usage to the full actions list and their options.
        Then trigger a parser error, which displays the help message.
        """
        usage = self.format_digest()
        self.parser.error("no action specified\n"+usage)

    def print_full_help(self):
        """
        Reset the parser usage to the full actions list and their options.
        Then trigger a parser error, which displays the help message.
        """
        if self.args is not None:
            return
        usage = self.format_desc()
        self.parser.error("no action specified\n"+usage)

    def print_short_help(self):
        """
        Reset the parser usage to a short message presenting only the most
        currently used actions. Then trigger a parser error, which displays the
        help message.
        """
        if self.args is not None:
            return
        highlight_actions = ["start", "stop", "print_status"]
        usage = self.format_desc(action=highlight_actions, options=False) + \
                "\n\nOptions:\n" + \
                "  -h, --help       Display more actions and options\n"
        self.parser.error("no action specified\n"+usage)

    def print_context_help(self, action, options):
        """
        Trigger a parser error, which displays the help message contextualized
        for the action prefix.
        """
        if options.parm_help:
            raise ex.Error(self.parser.format_help())
        else:
            usage = self.format_digest(action)
            raise ex.Error("%s" % usage)

