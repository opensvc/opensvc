import logging
from xml.etree.ElementTree import ElementTree, fromstring, Element

SC_CONFIG = '''
<service_bundle name="sysconfig" type="profile">
    <service name="system/identity" type="service" version="1">
        <instance enabled="true" name="cert"/>
        <instance enabled="true" name="node">
            <property_group name="config" type="application">
                <propval name="nodename" type="astring" value="skelzone"/>
            </property_group>
        </instance>
    </service>
    <service name="network/install" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="install_ipv6_interface" type="application">
                <propval name="stateful" type="astring" value="yes"/>
                <propval name="name" type="astring" value="net1/v6"/>
                <propval name="stateless" type="astring" value="yes"/>
                <propval name="address_type" type="astring" value="addrconf"/>
            </property_group>
        </instance>
    </service>
    <service name="network/dns/client" type="service" version="1">
        <property_group name="config" type="application">
        </property_group>
        <instance enabled="true" name="default"/>
    </service>
    <service name="system/name-service/cache" type="service" version="1">
        <instance enabled="true" name="default"/>
    </service>
    <service name="system/name-service/switch" type="service" version="1">
        <property_group name="config" type="application">
        </property_group>
        <instance enabled="true" name="default"/>
    </service>
    <service name="system/environment" type="service" version="1">
        <instance enabled="true" name="init">
            <property_group name="environment" type="application">
            </property_group>
        </instance>
    </service>
    <service name="system/timezone" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="timezone" type="application">
                <propval name="localtime" type="astring" value="Europe/Paris"/>
            </property_group>
        </instance>
    </service>
    <service name="system/config-user" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="root_account" type="application">
                <propval name="password" type="astring" value="NP"/>
                <propval name="type" type="astring" value="normal"/>
                <propval name="login" type="astring" value="root"/>
            </property_group>
        </instance>
    </service>
</service_bundle>
'''


def property_group(name, attr_type):
    return Element('property_group', attrib={'name': name, 'type': attr_type})


def propval(name, attr_type, value):
    return Element('propval', attrib={'name': name, 'type': attr_type, 'value': value})


class InstallIpv4Interface:
    def __init__(self, name, static_address=None, address_type='static',
                 default_route=None, id=0):
        """
        create fragment of xml system configuration profile for network
        if id is not 0, then use 'multiple interface' fragment
        """
        self.name = name
        self.static_address = static_address
        self.address_type = address_type
        self.default_route = default_route
        self.id = id

    def element(self):
        if self.id == 0:
            attr_type = 'application'
            name = 'install_ipv4_interface'
        else:
            attr_type = 'ipv4_interface'
            name = 'install_ipv4_interface_%s' % str(self.id)
        prop_group = property_group(name, attr_type)
        if self.default_route and self.id == 0:
            prop_group.append(propval('default_route', 'net_address_v4', self.default_route))
        if self.static_address:
            prop_group.append(propval('static_address', 'net_address_v4', self.static_address))
        if self.name:
            prop_group.append(propval('name', 'astring', self.name))
        if self.address_type:
            prop_group.append(propval('address_type', 'astring', self.address_type))
        return prop_group


class ScProfile:
    def __init__(self, sc_profile_file='sc_profile.xml', template=SC_CONFIG, log=None):
        self.sc_profile_file = sc_profile_file
        self.root = fromstring(template)
        self.log = log or logging.getLogger()

    def write(self):
        self.log.info('create system configuration profile %s', self.sc_profile_file)
        tree = ElementTree(element=self.root)
        tree.write(self.sc_profile_file, encoding='US-ASCII', xml_declaration=True)

    def set_nodename(self, nodename):
        path = ("service/[@name='system/identity']/instance/[@name='node']"
                "/property_group/[@name='config']/propval/[@name='nodename']")
        self.root.find(path).set('value', nodename)

    def add_ipv4_interface(self, install_ipv4_interface=None):
        if install_ipv4_interface is None:
            return
        network_install = self.root.find("service/[@name='network/install']/instance")
        network_install.append(install_ipv4_interface.element())

    def set_name_service_switch(self, name_values=None):
        path = "service/[@name='system/name-service/switch']/property_group/[@name='config']"
        ns_switch = self.root.find(path)
        for name, value in name_values.items():
            ns_switch.append(propval(name, 'astring', value))
        ns_switch.append(propval('default', 'astring', 'files'))

    def set_localtime(self, localtime):
        path = ("service/[@name='system/timezone']/instance/[@name='default']"
                "/property_group/[@name='timezone']/propval/[@name='localtime']")
        self.root.find(path).set('value', localtime)

    def set_environment(self, name_values):
        path = ("service/[@name='system/environment']/instance/[@name='init']"
                "/property_group/[@name='environment']")
        environment = self.root.find(path)
        for name, value in name_values.items():
            environment.append(propval(name, 'astring', value))

    def set_dns_client(self, searches=None, nameservers=None):
        path = "service/[@name='network/dns/client']/property_group/[@name='config']"
        dns_config = self.root.find(path)
        if nameservers:
            net_address_list = Element('net_address_list')
            for nameserver in nameservers:
                net_address_list.append(Element('value_node', attrib={'value': nameserver}))
            nameserver_element = Element('property', attrib={'name': 'nameserver',
                                                             'type': 'net_address'})
            nameserver_element.append(net_address_list)
            dns_config.append(nameserver_element)

        if searches:
            astring_list = Element('astring_list')
            for search in searches:
                astring_list.append(Element('value_node', attrib={'value': search}))
            search_element = Element('property', attrib={'name': 'search', 'type': 'astring'})
            search_element.append(astring_list)
            dns_config.append(search_element)
