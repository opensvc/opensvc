#!/usr/bin/env python

data = {
  "default_prefix": "OSVC_COMP_SYSTEMD_UNIT",
  "example_value": """ 
{
  "name": "lvm2-lvmetad.service",
  "disable": true,
  "mask": false
}
  """,
  "description": """* Controls a systemd unit masked and disabled states.
""",
  "form_definition": """
Desc: |
  A rule to set a systemd unit masked and disabled states.
Css: comp48

Outputs:
  -
    Dest: compliance variable
    Type: json
    Format: list of dict
    Class: systemd_unit_state

Inputs:
  -
    Id: name
    Label: Name
    DisplayModeLabel: name
    LabelCss: action16
    Mandatory: Yes
    Type: string
    Help: The systemd unit name, including the suffix (.service or .socket)

  -
    Id: disable
    Label: Disable
    DisplayModeLabel: disable
    LabelCss: action16
    Mandatory: Yes
    Default: No
    Type: boolean
    Help: Should the unit be disabled.

  -
    Id: mask
    Label: Mask
    DisplayModeLabel: mask
    LabelCss: action16
    Mandatory: Yes
    Default: No
    Type: boolean
    Help: Should the unit be masked.

""",
}

import os
import sys
import json
import pwd
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class SystemdUnitState(CompObject):
    def __init__(self, prefix=None):
        CompObject.__init__(self, prefix=prefix, data=data)

    def init(self):
        if os.uname()[0] != "Linux":
            raise NotApplicable()

        self.rules = []

        self.rules = self.get_rules()

        if len(self.rules) == 0:
            raise NotApplicable()

        self.unit_data = {}

    def mask(self, unit):
        return self.systemctl("mask", unit)

    def unmask(self, unit):
        return self.systemctl("unmask", unit)

    def enable(self, unit):
        return self.systemctl("enable", unit)

    def disable(self, unit):
        return self.systemctl("disable", unit)

    def systemctl(self, action, unit):
        cmd = ["systemctl", action, unit, "--now"]
        pinfo("systemd_unit_state:", " ".join(cmd))
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        _, err = p.communicate()
        err = bdecode(err)
        if p.returncode != 0:
            perror("systemd unit: command failed (%d): %s" % (p.returncode, err))
            return RET_ERR
        return RET_OK

    def show(self, unit):
        if unit in self.unit_data:
            return self.unit_data[unit]
        cmd = ["systemctl", "show", unit]
        try:
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out, _ = p.communicate()
        except OSError as exc:
            if exc.errno == 2:
                raise NotApplicable("systemd_unit_state: systemctl is not installed")

        out = bdecode(out)
        data = {}
        for line in out.splitlines():
            k, v = line.split("=", 1)
            data[k] = v
        self.unit_data[unit] = data
        return data

    def fixable(self):
        return RET_OK

    def check_rule(self, rule, verbose=False):
        r = RET_OK
        try:
            name = rule["name"]
        except IndexError:
            perror("invalid rule: no unit name")
            return RET_NA
        data = self.show(name)
        if data.get("LoadState") == "not-found":
            if verbose:
                pinfo("systemd_unit_state: %s not found" % name)
            return RET_OK

        if "mask" in rule:
            if rule["mask"]:
                if data["LoadState"] != "masked":
                    if verbose:
                        perror("systemd_unit_state: %s should be masked" % name)
                    r |= RET_ERR
                else:
                    if verbose:
                        pinfo("systemd_unit_state: %s is masked" % name)
            if not rule["mask"]:
                if data["LoadState"] == "masked":
                    if verbose:
                        perror("systemd_unit_state: %s should not be masked" % name)
                    r |= RET_ERR
                else:
                    if verbose:
                        pinfo("systemd_unit_state: %s is unmasked" % name)
        if "disable" in rule and not data["LoadState"] == "masked":
            if rule["disable"]:
                if data["UnitFileState"] != "disabled":
                    if verbose:
                        perror("systemd_unit_state: %s should be disabled" % name)
                    r |= RET_ERR
                else:
                    if verbose:
                        pinfo("systemd_unit_state: %s is disabled" % name)
            if not rule["disable"]:
                if data["UnitFileState"] == "disabled":
                    if verbose:
                        perror("systemd_unit_state: %s should not be disabled" % name)
                    r |= RET_ERR
                else:
                    if verbose:
                        pinfo("systemd_unit_state: %s is enabled" % name)
        return r

    def check(self):
        r = RET_OK
        for rule in self.rules:
            r |= self.check_rule(rule, verbose=True)
        return r

    def fix_rule(self, rule):
        r = RET_OK
        try:
            name = rule["name"]
        except IndexError:
            pinfo("systemd unit: invalid rule: no unit name")
            return RET_NA
        data = self.show(name)
        if "mask" in rule:
            if rule["mask"] and data["LoadState"] != "masked":
                r |= self.mask(name)
            if not rule["mask"] and data["LoadState"] == "masked":
                r |= self.unmask(name)
        if "disable" in rule and not rule.get("mask"):
            # can not disable a masked unit
            if rule["disable"] and data["UnitFileState"] != "disabled":
                r |= self.disable(name)
            if not rule["disable"] and data["UnitFileState"] == "disabled":
                r |= self.enable(name)
        return r

    def fix(self):
        r = 0
        for rule in self.rules:
            if self.check_rule(rule, verbose=False) == RET_ERR:
                r |= self.fix_rule(rule)
        return r

if __name__ == "__main__":
    main(SystemdUnitState)
