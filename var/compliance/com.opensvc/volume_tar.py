#!/usr/bin/env python

from __future__ import print_function

data = {
  "default_prefix": "OSVC_COMP_VOLUME_TAR",
  "example_value": """                                                                                                                                                     
{                                                                                                                                                                          
  "ref": "/some/path/to/file.tar",                                                                                                                                         
  "path": "/home/user/bin",                                                                                                                                                
  "immutable": "true"                                                                                                                                                      
}                                                                                                                                                                          
  """,                                                                                                                                                                     
  "description": """* Verify and install tar content in a docker volume spectified by the environment variable OPENSVC_VOL_PATH, automatically set by the fs.docker driver provisioner.
* Paths are relative to the volume head
* Verify tar archive is extracted on check action                                                                                                                          
* Extract tar archive on fix action                                                                                                                                        
* Immutable boolean is used to know if extracted tar content can be modified on filesystem                                                                                 
""",                                                                                                                                                                       
  "form_definition": """
Desc: |
  A volume_tar rule, fed to the 'tar' compliance object to extract archive inside docker volume. For tar files, a reference content must be specified or pointed through an URL.
Css: comp48

Outputs:
  -
    Dest: compliance variable
    Class: file
    Type: json
    Format: dict

Inputs:
  -
    Id: ref
    Label: Tar uri
    DisplayModeLabel: ref
    LabelCss: fa-map-marker
    Help: "Examples:
        /path/to/reference_file.tar
        safe://safe.uuid.8dc85529a2b13b4b.626172.tar
        http://server/path/to/reference_file.tar
        https://server/path/to/reference_file.tar
        ftp://server/path/to/reference_file.tar
        ftp://login:pass@server/path/to/reference_file.tar"
    Type: string
  -
    Id: path
    Label: Install path
    DisplayModeLabel: path
    LabelCss: fa-map-marker
    Mandatory: Yes
    Help: path to install the tar reference content to.
    Type: string
  -
    Id: immutable
    Label: Immutable
    DisplayModeLabel: immutable
    LabelCss: fa-lock
    Mandatory: Yes
    Help: "On : extracted tar archive must not be modified on filesystem
           Off: extracted tar archive contents on filesystem can be modified"
    Type: boolean
"""
}

import os
import sys

sys.path.append(os.path.dirname(__file__))

from comp import *
from tar import Tar

class CompVolumeTar(Tar):
    def __init__(self, prefix=None):
        CompObject.__init__(self, prefix=prefix, data=data)

    def init(self):
        if "OPENSVC_VOL_PATH" not in os.environ:
            raise NotApplicable()
        Tar.init(self)
        self.vol_path = os.environ["OPENSVC_VOL_PATH"]
        for rule in self.rules:
            rule["path"] = os.path.join(self.vol_path, rule["path"].lstrip(os.sep))

if __name__ == "__main__":
    main(CompVolumeTar)
