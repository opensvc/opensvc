import os
import socket

import foreign.six as six
import core.exceptions as ex
from utilities.proc import justcall, which

class Nsr(object):
    def __init__(self):
        if not which('mminfo'):
            raise ex.Error('mminfo not found')
        self.keys = ['mminfo']

    def get_mminfo(self):
        os.environ["LC_TIME"] = "en_DK"
        cmd = ['mminfo', '-x', 'c;', '-q', 'savetime>=last day', '-r', 'client,name,group,totalsize,savetime(30),ssretent(30),volume,level,ssid(53)']
        print(' '.join(cmd))
        lines = justcall(cmd)[0].split('\n')[1:]
        for li, line in enumerate(lines):
            if len(line) == 0:
                continue
            try:
                i = line.index(';')
            except ValueError:
                continue
            client = line[:i]
            try:
                a = socket.getaddrinfo(client, None)
            except socket.gaierror:
                a = []
            if len(a) > 0:
                ip = a[0][-1][0]
            else:
                ip = client
            lines[li] = ip + line[i:]
        return six.text_type('\n'.join(lines), errors='ignore')


if __name__ == "__main__":
    o = Nsr()
    print(o.get_mminfo())
