import resources as Res
import rcStatus
import re
import os
from rcGlobalEnv import rcEnv
import rcExceptions as ex

class Stonith(Res.Resource):
    def __init__(self, **kwargs):
        Res.Resource.__init__(self, **kwargs)
        self.name = None
        self.re_login = re.compile("(login\s*: )|(Login Name:  )|(username: )|(User Name :)", re.IGNORECASE)
        self.re_pass  = re.compile("password", re.IGNORECASE)

    def creds(self):
        import ConfigParser
        c = ConfigParser.RawConfigParser()
        c.read(os.path.join(rcEnv.paths.pathetc, 'auth.conf'))

        username = None
        password = None
        key = None

        if not c.has_section(self.name):
            raise ex.excError("No credentials in node.conf for %s"%self.name)

        if c.has_option(self.name, "username"):
            username = c.get(self.name, 'username')
        else:
            raise ex.excError("No username in node.conf for %s"%self.name)

        if c.has_option(self.name, "password"):
            password = c.get(self.name, 'password')
        if c.has_option(self.name, "key"):
            key = c.get(self.name, 'key')
            if not os.path.exists(key):
                raise ex.excError("key in node.conf for %s does not exist"%self.name)

        if password is None and key is None:
            raise ex.excError("No password nor key in node.conf for %s"%self.name)

        return username, password, key

    def start(self):
        if self.sanity():
            self.log.info("sanity checks passed. trigger stonith method")
        else:
            self.log.info("stonith bypassed")
            return
        self._start()

    def _start(self):
        pass

    def _status(self, verbose=False):
        return rcStatus.NA

    def sanity(self):
        for resource in self.svc.get_resources(['hb.ovm', 'hb.openha', 'hb.linuxha']):
            if not resource.disabled and hasattr(resource, 'need_stonith') and resource.need_stonith():
                self.log.info("heartbeat %s asks for stonith" % resource.rid)
                return True
        self.log.debug("no heartbeat asks for stonith")
        return False

