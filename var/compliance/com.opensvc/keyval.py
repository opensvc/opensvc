#!/usr/bin/env python

data = {
  "default_prefix": "OSVC_COMP_GROUP_",
  "example_kwargs": {
    "path": "/etc/ssh/sshd_config",
  },
  "example_value": """
[
  {
    "key": "PermitRootLogin",
    "op": "=",
    "value": "yes"
  }
]
""",
  "description": """* Setup and verify keys in "key value" formatted configuration file.
* Example files: sshd_config, ssh_config, ntp.conf, ...
""",
  "form_definition": """
Desc: |
  A rule to set a list of parameters in simple keyword/value configuration file format. Current values can be checked as set or unset, strictly equal, or superior/inferior to their target value.
Outputs:
  -
    Dest: compliance variable
    Type: json
    Format: list of dict
    Class: keyval
Inputs:
  -
    Id: key
    Label: Key
    DisplayModeTrim: 64
    DisplayModeLabel: key
    LabelCss: action16
    Mandatory: Yes
    Type: string
    Help:
  -
    Id: op
    Label: Comparison operator
    DisplayModeLabel: op
    LabelCss: action16
    Mandatory: Yes
    Type: string
    Default: "="
    Candidates:
      - reset
      - unset
      - "="
      - ">"
      - ">="
      - "<"
      - "<="
    Help: The comparison operator to use to check the parameter current value.
  -
    Id: value
    Label: Value
    DisplayModeLabel: value
    LabelCss: action16
    Mandatory: Yes
    Type: string or integer
    Help: The configuration file parameter target value.
""",
}


import os
import sys
import json

sys.path.append(os.path.dirname(__file__))

from comp import *
from keyval_parser import Parser, ParserError

class KeyVal(CompObject):
    def __init__(self, prefix=None, path=None):
        CompObject.__init__(self, prefix=prefix, data=data)
        self.cf = path

    def init(self):
        self.nocf = False
        if self.cf is None:
            perror("no file path specified")
            raise NotApplicable()

        self.keys = self.get_rules()

        self.target_n_key = {}
        for i, key in enumerate(self.keys):
             if self.keys[i]['op'] == 'IN':
                 self.keys[i]['value'] = json.loads(self.keys[i]['value'])
             if 'op' in key and 'key' in key and key['op'] not in ("unset", "reset"):
                 if key['key'] not in self.target_n_key:
                     self.target_n_key[key['key']] = 1
                 else:
                     self.target_n_key[key['key']] += 1
        try:
            self.conf = Parser(self.cf)
        except ParserError as e:
            perror(e)
            raise ComplianceError()


    def fixable(self):
        return RET_OK

    def _check_key(self, keyname, target, op, value, instance=0, verbose=True):
        r = RET_OK
        if op == "reset":
            if value is not None:
                current_n_key = len(value)
                target_n_key = self.target_n_key[keyname] if keyname in self.target_n_key else 0
                if current_n_key > target_n_key:
                    if verbose:
                        perror("%s is set %d times, should be set %d times"%(keyname, current_n_key, target_n_key))
                    return RET_ERR
                else:
                    if verbose:
                        pinfo("%s is set %d times, on target"%(keyname, current_n_key))
                    return RET_OK
            else:
                return RET_OK
        elif op == "unset":
            if value is not None:
                if target.strip() == "":
                    if verbose:
                        perror("%s is set, should not be"%keyname)
                    return RET_ERR
                target_found = False
                for i, val in enumerate(value):
                    if target == val:
                        target_found = True
                        break

                if target_found:
                    if verbose:
                        perror("%s[%d] is set to value %s, should not be"%(keyname, i, target))
                    return RET_ERR
                else:
                    if verbose:
                        pinfo("%s is not set to value %s, on target"%(keyname, target))
                    return RET_OK
            else:
                if target.strip() != "":
                    if verbose:
                        pinfo("%s=%s is not set, on target"%(keyname, target))
                else:
                    if verbose:
                        pinfo("%s is not set, on target"%keyname)
                return RET_OK

        if value is None:
            if op == 'IN' and "unset" in map(str, target):
                if verbose:
                    pinfo("%s is not set, on target"%(keyname))
                return RET_OK
            else:
                if verbose:
                    perror("%s[%d] is not set, target: %s"%(keyname, instance, str(target)))
                return RET_ERR

        if type(value) == list:
            if str(target) in value:
                if verbose:
                    pinfo("%s[%d]=%s on target"%(keyname, instance, str(value)))
                return RET_OK
            else:
                if verbose:
                    perror("%s[%d]=%s is not set"%(keyname, instance, str(target)))
                return RET_ERR

        if op == '=':
            if str(value) != str(target):
                if verbose:
                    perror("%s[%d]=%s, target: %s"%(keyname, instance, str(value), str(target)))
                r |= RET_ERR
            elif verbose:
                pinfo("%s=%s on target"%(keyname, str(value)))
        elif op == 'IN':
            if str(value) not in map(str, target):
                if verbose:
                    perror("%s[%d]=%s, target: %s"%(keyname, instance, str(value), str(target)))
                r |= RET_ERR
            elif verbose:
                pinfo("%s=%s on target"%(keyname, str(value)))
        else:
            if type(value) != int:
                if verbose:
                    perror("%s[%d]=%s value must be integer"%(keyname, instance, str(value)))
                r |= RET_ERR
            elif op == '<=' and value > target:
                if verbose:
                    perror("%s[%d]=%s target: <= %s"%(keyname, instance, str(value), str(target)))
                r |= RET_ERR
            elif op == '>=' and value < target:
                if verbose:
                    perror("%s[%d]=%s target: >= %s"%(keyname, instance, str(value), str(target)))
                r |= RET_ERR
            elif verbose:
                pinfo("%s[%d]=%s on target"%(keyname, instance, str(value)))
        return r

    def check_key(self, key, instance=0, verbose=True):
        if 'key' not in key:
            if verbose:
                perror("'key' not set in rule %s"%str(key))
            return RET_NA
        if 'value' not in key:
            if verbose:
                perror("'value' not set in rule %s"%str(key))
            return RET_NA
        if 'op' not in key:
            op = "="
        else:
            op = key['op']
        target = key['value']

        allowed_ops = ('>=', '<=', '=', 'unset', 'reset', 'IN')
        if op not in allowed_ops:
            if verbose:
                perror(key['key'], "'op' value must be one of", ", ".join(allowed_ops))
            return RET_NA

        keyname = key['key']
        value = self.conf.get(keyname, instance=instance)

        r = self._check_key(keyname, target, op, value, instance=instance, verbose=verbose)

        return r

    def fix_key(self, key, instance=0):
        if key['op'] == "unset" or (key['op'] == "IN" and key['value'][0] == "unset"):
            pinfo("%s unset"%key['key'])
            if key['op'] == "IN":
                target = None
            else:
                target = key['value']
            self.conf.unset(key['key'], target)
        elif key['op'] == "reset":
            target_n_key = self.target_n_key[key['key']] if key['key'] in self.target_n_key else 0
            pinfo("%s truncated to %d definitions"%(key['key'], target_n_key))
            self.conf.truncate(key['key'], target_n_key)
        else:
            if key['op'] == "IN":
                target = key['value'][0]
            else:
                target = key['value']
            pinfo("%s=%s set"%(key['key'], target))
            self.conf.set(key['key'], target, instance=instance)

    def check(self):
        r = 0
        key_instance = {}
        for key in self.keys:
            if 'key' not in key or 'op' not in key:
                continue
            if key['op'] in ('reset', 'unset'):
                instance = None
            else:
                if key['key'] not in key_instance:
                    key_instance[key['key']] = 0
                else:
                    key_instance[key['key']] += 1
                instance = key_instance[key['key']]
            r |= self.check_key(key, instance=instance, verbose=True)
        return r

    def fix(self):
        key_instance = {}
        for key in self.keys:
            if 'key' not in key or 'op' not in key:
                continue
            if key['op'] in ('reset', 'unset'):
                instance = None
            else:
                if key['key'] not in key_instance:
                    key_instance[key['key']] = 0
                else:
                    key_instance[key['key']] += 1
                instance = key_instance[key['key']]
            if self.check_key(key, instance=instance, verbose=False) == RET_ERR:
                self.fix_key(key, instance=instance)
        if not self.conf.changed:
            return
        try:
            self.conf.write()
        except ParserError as e:
            perror(e)
            return RET_ERR
        return RET_OK

if __name__ == "__main__":
    main(KeyVal)
