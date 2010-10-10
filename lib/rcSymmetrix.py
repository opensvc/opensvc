from rcUtilities import justcall, which
from xml.etree.ElementTree import XML, fromstring
import rcExceptions as ex
import os

class Syms(object):
    syms = []
    def __init__(self):
        self.index = 0
        if which('symcfg') is None:
            print 'Can not find symcli programs in PATH'
            raise ex.excError
        out, err, ret = justcall(['symcfg', 'list', '-output', 'xml_element'])
        if ret != 0:
            print err
            raise ex.excError
        tree = fromstring(out)
        for symm in tree.getiterator('Symm_Info'):
            model = symm.find('model').text
            sid = symm.find('symid').text
            if model in ['VMAX-1']:
                self.syms.append(Vmax(sid))

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.syms):
            raise StopIteration
        self.index += 1
        return self.syms[self.index-1]

class Vmax(object):
    keys = ['sym_info',
            'sym_dir_info',
            'sym_dev_info',
            'sym_ficondev_info',
            'sym_meta_info',
            'sym_disk_info',
            'sym_diskgroup_info',
            'sym_fa_info',
            'sym_ig_aclx',
            'sym_pg_aclx',
            'sym_sg_aclx',
            'sym_view_aclx']

    def __init__(self, sid):
        self.sid = sid
        if 'SYMCLI_DB_FILE' in os.environ:
            # flat format
            self.aclx = os.path.join(os.path.dirname(os.environ['SYMCLI_DB_FILE']), sid+'.aclx')
            if not os.path.exists(self.aclx):
                # emc grab format
                self.aclx = os.path.join(os.path.dirname(os.environ['SYMCLI_DB_FILE']), sid, sid+'.aclx')
            if not os.path.exists(self.aclx):
                print "missing file %s"%self.aclx
        else:
            self.aclx = None

    def symcmd(self, cmd):
        cmd += ['-sid', self.sid, '-output', 'xml_element']
        return justcall(cmd)

    def symaccesscmd(self, cmd):
        if self.aclx is None:
            cmd += ['-output', 'xml_element']
        else:
            cmd += ['-file', self.aclx, '-output', 'xml_element']
        return justcall(cmd)

    def get_sym_info(self):
        cmd = ['symcfg', 'list']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_dir_info(self):
        cmd = ['symcfg', '-dir', 'all', '-v', 'list']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_dev_info(self):
        cmd = ['symdev', 'list']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_ficondev_info(self):
        cmd = ['symdev', 'list', '-ficon']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_meta_info(self):
        cmd = ['symdev', 'list', '-meta', '-v']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_disk_info(self):
        cmd = ['symdisk', 'list', '-v']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_diskgroup_info(self):
        cmd = ['symdisk', 'list', '-dskgrp_summary']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_fa_info(self):
        cmd = ['symcfg', '-fa', 'all', 'list', '-v']
        out, err, ret = self.symcmd(cmd)
        return out

    def get_sym_pg_aclx(self):
        cmd = ['symaccess', 'list', '-type', 'port']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def get_sym_sg_aclx(self):
        cmd = ['symaccess', 'list', '-type', 'storage']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def get_sym_ig_aclx(self):
        cmd = ['symaccess', 'list', '-type', 'initiator']
        out, err, ret = self.symaccesscmd(cmd)
        return out

    def get_sym_view_aclx(self):
        cmd = ['symaccess', 'list', 'view', '-details']
        out, err, ret = self.symaccesscmd(cmd)
        return out

