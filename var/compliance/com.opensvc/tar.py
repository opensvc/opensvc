#!/usr/bin/env python

data = {
  "default_prefix": "OSVC_COMP_TAR_",
  "example_value": """ 
{
  "ref": "/some/path/to/file.tar",
  "path": "/home/user/bin",
  "immutable": "true"
}
  """,
  "description": """* Fetch a tar archive from a href
* Verify tar archive is extracted on check action
* Extract tar archive on fix action
* Immutable boolean is used to know if extracted tar content can be modified on filesystem
""",
  "form_definition": """
Desc: |
  Point to a tar archive.
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
import tempfile
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class InitError(Exception):
    pass

class Tar(CompObject):
    def __init__(self, prefix=None):
        CompObject.__init__(self, prefix=prefix, data=data)

    def init(self):
        self.rules = []

        for rule in self.get_rules():
            try:
                self.rules += self.add_rule(rule)
            except InitError:
                continue
            except ValueError:
                perror('tar_archive: failed to parse variable', os.environ[k])

        if len(self.rules) == 0:
            raise NotApplicable()

    def add_rule(self, d):
        if 'path' not in d or 'ref' not in d:
            perror('tar_archive: path and ref must be in the dict:', d)
            RET = RET_ERR
            return []
        return [d]

    def download(self, d):
        if 'ref' in d and d['ref'].startswith("safe://"):
            return self.get_safe_file(d["ref"])
        else:
            return self.download_url(d)

    def download_url(self, d):
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        try:
            self.urlretrieve(d['ref'], tmpf)
        except IOError as e:
            perror("file ref", d['ref'], "download failed:", e)
            raise InitError()
        return tmpf

    def get_safe_file(self, uuid):
        tmpf = tempfile.NamedTemporaryFile()
        tmpfname = tmpf.name
        tmpf.close()
        try:
            self.collector_safe_file_download(uuid, tmpfname)
        except Exception as e:
            raise ComplianceError("%s: %s" % (uuid, str(e)))
        return tmpfname

    def check_output(self, data, verbose=False):
        lines = [line for line in data.splitlines() if \
                 "Mod time differs" not in line and "Size differs" not in line]
        nberr = len(lines)
        if nberr:
            if verbose:
                for line in lines[:10]:
                     perror(line)
                if nberr > 10:
                     perror("... %d total errors" % nberr)
            return RET_ERR
        return RET_OK

    def fixable(self):
        return RET_NA

    def fix_tarball(self, rule, verbose=False):
        tmpfname = self.download(rule)
        path = rule["path"]
        immutable = rule["immutable"]
        try:
            return self._fix_tarball(rule, tmpfname, path, immutable, verbose=verbose)
        finally:
            os.unlink(tmpfname)

    def _fix_tarball(self, rule, tmpfname, path, immutable, verbose=False):
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except Exception as e:
                raise ex.excError("failed to create directory %s: %s"%(path, str(e)))
        opts = '--keep-newer-files'
        if immutable is True:
            opts = '--overwrite'
        cmd = ["tar", "-C", path, "--extract", "--file", tmpfname, opts]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        pinfo(out)
        perror(err)
        if proc.returncode == 0:
            return RET_OK
        return RET_ERR

    def check_tarball(self, rule, verbose=False):
        tmpfname = self.download(rule)
        path = rule["path"]
        immutable = rule["immutable"]
        try:
            return self._check_tarball(rule, tmpfname, path, immutable, verbose=verbose)
        finally:
            os.unlink(tmpfname)

    def _check_tarball(self, rule, tmpfname, path, immutable, verbose=False):
        cmd = ["tar", "-C", path, "--compare", "--file", tmpfname]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        out = bdecode(out)
        err = bdecode(err)
        if proc.returncode == 0:
            return RET_OK
        elif immutable is False:
            return self.check_output(out+err, verbose=verbose)
        else:
            pinfo(out)
            perror(err)
        return RET_ERR

    def check(self):
        r = 0
        for rule in self.rules:
            r |= self.check_tarball(rule, verbose=True)
        return r

    def fix(self):
        r = 0
        for rule in self.rules:
            r |= self.fix_tarball(rule)
        return r

if __name__ == "__main__":
    main(Tar)
