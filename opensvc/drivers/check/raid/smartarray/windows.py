import os

from . import Check as BaseCheck

sep = ';'
path_list = os.environ['PATH'].split(sep)

if 'PROGRAMFILES' in os.environ:
    path_list.append(os.path.join(os.environ.get('PROGRAMFILES'),
                                  'compaq', 'hpacucli', 'bin'))
if 'PROGRAMFILES(X86)' in os.environ:
    path_list.append(os.path.join(os.environ.get('PROGRAMFILES(X86)'),
                                  'compaq', 'hpacucli', 'bin'))

os.environ['PATH'] = sep.join(path_list)

class Check(BaseCheck):
    pass

