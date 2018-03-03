#!/usr/bin/env python

from __future__ import print_function

data = {
  "default_prefix": "OSVC_COMP_VOLUME_FILE_",
  "example_value": """ 
{
  "path": "/some/path/to/file",
  "fmt": "root@corp.com		%%HOSTNAME%%@corp.com",
  "uid": 500,
  "gid": 500,
}
  """,
  "description": """* Verify and install file content in a docker volume spectified by the environment variable OPENSVC_VOL_PATH, automatically set by the fs.docker driver provisioner.
* paths are relative to the volume head
* Verify and set file or directory ownership and permission
* Directory mode is triggered if the path ends with /

Special wildcards::

  %%ENV:VARNAME%%	Any environment variable value
  %%HOSTNAME%%		Hostname
  %%SHORT_HOSTNAME%%	Short hostname

""",
  "form_definition": """
Desc: |
  A file rule, fed to the 'files' compliance object to create a directory or a file and set its ownership and permissions. For files, a reference content can be specified or pointed through an URL.
Css: comp48

Outputs:
  -
    Dest: compliance variable
    Class: file
    Type: json
    Format: dict

Inputs:
  -
    Id: path
    Label: Path
    DisplayModeLabel: path
    LabelCss: action16
    Mandatory: Yes
    Help: File path to install the reference content to. A path ending with '/' is treated as a directory and as such, its content need not be specified.
    Type: string

  -
    Id: mode
    Label: Permissions
    DisplayModeLabel: perm
    LabelCss: action16
    Help: "In octal form. Example: 644"
    Type: integer

  -
    Id: uid
    Label: Owner
    DisplayModeLabel: uid
    LabelCss: guy16
    Help: Either a user ID or a user name
    Type: string or integer

  -
    Id: gid
    Label: Owner group
    DisplayModeLabel: gid
    LabelCss: guy16
    Help: Either a group ID or a group name
    Type: string or integer

  -
    Id: ref
    Label: Content URL pointer
    DisplayModeLabel: ref
    LabelCss: loc
    Help: "Examples:
        http://server/path/to/reference_file
        https://server/path/to/reference_file
        ftp://server/path/to/reference_file
        ftp://login:pass@server/path/to/reference_file"
    Type: string

  -
    Id: fmt
    Label: Content
    DisplayModeLabel: fmt
    LabelCss: hd16
    Css: pre
    Help: A reference content for the file. The text can embed substitution variables specified with %%ENV:VAR%%.
    Type: text
"""
}

import os
import sys

sys.path.append(os.path.dirname(__file__))

from comp import *
from file import CompFiles

class CompVolumeFiles(CompFiles):
    def __init__(self, prefix=None):
        CompObject.__init__(self, prefix=prefix, data=data)

    def init(self):
        if "OPENSVC_VOL_PATH" not in os.environ:
            raise NotApplicable()
        CompFiles.init(self)
        self.vol_path = os.environ["OPENSVC_VOL_PATH"]
        for i, data in enumerate(self.files):
            self.files[i]["path"] = os.path.join(self.vol_path, data["path"].lstrip(os.sep))

if __name__ == "__main__":
    main(CompVolumeFiles)
