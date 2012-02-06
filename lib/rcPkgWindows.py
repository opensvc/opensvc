#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#

from collections import namedtuple
from ctypes import byref, create_unicode_buffer, windll
from ctypes.wintypes import DWORD
from itertools import count
 
# defined at http://msdn.microsoft.com/en-us/library/aa370101(v=VS.85).aspx
UID_BUFFER_SIZE = 39
PROPERTY_BUFFER_SIZE = 256 
ERROR_MORE_DATA = 234
ERROR_INVALID_PARAMETER = 87
ERROR_SUCCESS = 0
ERROR_NO_MORE_ITEMS = 259 
ERROR_UNKNOWN_PRODUCT = 1605 
  
# diff propoerties of a product, not all products have all properties
PRODUCT_PROPERTIES = [u'Language',
                      u'ProductName',
                      u'PackageCode',
                      u'Transforms',
                      u'AssignmentType',
                      u'PackageName',
                      u'InstalledProductName',
                      u'VersionString',
                      u'RegCompany',
                      u'RegOwner',
                      u'ProductID',
                      u'ProductIcon',
                      u'InstallLocation',
                      u'InstallSource',
                      u'InstallDate',
                      u'Publisher',
                      u'LocalPackage',
                      u'HelpLink',
                      u'HelpTelephone',
                      u'URLInfoAbout',
                      u'URLUpdateInfo',] 

# class to be used for python users :)
Product = namedtuple('Product', PRODUCT_PROPERTIES)
 
def get_property_for_product(product, property, buf_size=PROPERTY_BUFFER_SIZE):
    """Retruns the value of a fiven property from a product."""
    property_buffer = create_unicode_buffer(buf_size)
    size = DWORD(buf_size)
    result = windll.msi.MsiGetProductInfoW(product, property, property_buffer,
                                           byref(size))
    if result == ERROR_MORE_DATA:
        return get_property_for_product(product, property,
                                        2 * buf_size)
    elif result == ERROR_SUCCESS:
        return property_buffer.value
    else:
        return None

def populate_product(uid):
    """Return a Product with the different present data."""
    properties = []
    for property in PRODUCT_PROPERTIES:
        properties.append(get_property_for_product(uid, property))
    return Product(*properties) 

def get_installed_products_uids():
    """Returns a list with all the different uid of the installed apps."""
    # enum will return an error code according to the result of the app
    products = []
    for i in count(0):
        uid_buffer = create_unicode_buffer(UID_BUFFER_SIZE)
        result = windll.msi.MsiEnumProductsW(i, uid_buffer)
        if result == ERROR_NO_MORE_ITEMS:
            # done interating over the collection
            break
        products.append(uid_buffer.value)
    return products
 
 
def get_installed_products():
    """Returns a collection of products that are installed in the system."""
    products = []
    for puid in  get_installed_products_uids():
        products.append(populate_product(puid))
    return products 
 
 
def is_product_installed_uid(uid):
    """Return if a product with the given id is installed.
 
    uid Most be a unicode object with the uid of the product using
    the following format {uid}
    """
    # we try to get the VersisonString for the uid, if we get an error it means
    # that the product is not installed in the system.
    buf_size = 256
    uid_buffer = create_unicode_buffer(uid)
    property = u'VersionString'
    property_buffer = create_unicode_buffer(buf_size)
    size = DWORD(buf_size)
    result = windll.msi.MsiGetProductInfoW(uid_buffer, property, property_buffer,
                                           byref(size))
    if result == ERROR_UNKNOWN_PRODUCT:
        return False
    else:
        return True

from rcGlobalEnv import rcEnv

def listpkg():
    plist = get_installed_products()
    lines = []
    for p in plist:
        x = [rcEnv.nodename,
	     p.ProductName,
	     p.VersionString,
	     ""]
        lines.append(x)
    return lines


def listpatch():
    return [] 

if __name__ == "__main__":
    print get_installed_products()
