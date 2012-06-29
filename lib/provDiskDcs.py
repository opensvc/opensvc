import rcExceptions as ex
import rcDcs

"""
{
 u'paths': u'iqn.1994-05.com.domain:linux-iqn.2000-08.com.datacore:sds1-1,iqn.1994-05.com.domain:linux-iqn.2000-08.com.datacore:sds2-1',
 u'rtype': u'disk',
 u'array_model': u'SANsymphony-V',
 u'dg_name': u'sds1_pool1',
 u'caption': u'foo',
 u'type': u'dcs',
 u'array_name':
 u'local',
 u'size': 10
}
"""
def d_provisioner(data):
    o = rcDcs.Dcss()
    if 'array_name' not in data:
        raise ex.excError("'array_name' key is mandatory")
    dcs = o.get_dcs(data['array_name'])
    if dcs is None:
        raise ex.excError("no dcs found in domain %s to provision the disk"%data['array_name'])
    dcs.add_vdisk(data)
