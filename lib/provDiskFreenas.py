import rcExceptions as ex
import rcFreenas

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
    o = rcFreenas.Freenass()
    if 'array_name' not in data:
        raise ex.excError("'array_name' key is mandatory")
    array = o.get_freenas(data['array_name'])
    if array is None:
        raise ex.excError("array %s not found to provision the disk"%data['array_name'])
    array.add_disk(data)
