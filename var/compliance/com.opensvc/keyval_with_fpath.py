#!/usr/bin/env python

data = {
  "default_prefix": "OSVC_COMP_GROUP_",
  "example_value": """
{
  "path": "/etc/ssh/sshd_config",
  "keys": [
    {
      "key": "PermitRootLogin",
      "op": "=",
      "value": "yes"
    },
    {
      "key": "PermitRootLogin",
      "op": "reset",
      "value": ""
    }
  ]
}

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
    Format: dict
    Class: keyval_with_fpath

Inputs:
  -
    Id: path
    Label: Path
    DisplayModeLabel: path
    LabelCss: file16
    Type: string
  -
    Id: keys
    Label: Keys
    DisplayModeLabel: keys
    LabelCss: key
    Type: form
    Form: keyval
""",
}


import os
import sys
import json

sys.path.append(os.path.dirname(__file__))

from comp import *
from keyval_parser import Parser, ParserError
import keyval

class KeyVal(keyval.KeyVal):
    def __init__(self, prefix=None, path=None):
        CompObject.__init__(self, prefix=prefix, data=data)
        self.cf = path

if __name__ == "__main__":
    main(KeyVal)
