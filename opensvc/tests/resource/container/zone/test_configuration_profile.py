import os

import pytest

from drivers.resource.container.zone.configuration_profile import ScProfile, InstallIpv4Interface

EXPECTED_PROFILE_DEFAULT = '''<?xml version='1.0' encoding='US-ASCII'?>
<service_bundle name="sysconfig" type="profile">
    <service name="system/identity" type="service" version="1">
        <instance enabled="true" name="cert" />
        <instance enabled="true" name="node">
            <property_group name="config" type="application">
                <propval name="nodename" type="astring" value="skelzone" />
            </property_group>
        </instance>
    </service>
    <service name="network/install" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="install_ipv6_interface" type="application">
                <propval name="stateful" type="astring" value="yes" />
                <propval name="name" type="astring" value="net1/v6" />
                <propval name="stateless" type="astring" value="yes" />
                <propval name="address_type" type="astring" value="addrconf" />
            </property_group>
        </instance>
    </service>
    <service name="network/dns/client" type="service" version="1">
        <property_group name="config" type="application">
        </property_group>
        <instance enabled="true" name="default" />
    </service>
    <service name="system/name-service/cache" type="service" version="1">
        <instance enabled="true" name="default" />
    </service>
    <service name="system/name-service/switch" type="service" version="1">
        <property_group name="config" type="application">
        </property_group>
        <instance enabled="true" name="default" />
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
                <propval name="localtime" type="astring" value="Europe/Paris" />
            </property_group>
        </instance>
    </service>
    <service name="system/config-user" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="root_account" type="application">
                <propval name="password" type="astring" value="NP" />
                <propval name="type" type="astring" value="normal" />
                <propval name="login" type="astring" value="root" />
            </property_group>
        </instance>
    </service>
</service_bundle>'''  # nopep8


EXPECTED_PROFILE_CUSTOM = '''<?xml version='1.0' encoding='US-ASCII'?>
<service_bundle name="sysconfig" type="profile">
    <service name="system/identity" type="service" version="1">
        <instance enabled="true" name="cert" />
        <instance enabled="true" name="node">
            <property_group name="config" type="application">
                <propval name="nodename" type="astring" value="skelzone-opensvc" />
            </property_group>
        </instance>
    </service>
    <service name="network/install" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="install_ipv6_interface" type="application">
                <propval name="stateful" type="astring" value="yes" />
                <propval name="name" type="astring" value="net1/v6" />
                <propval name="stateless" type="astring" value="yes" />
                <propval name="address_type" type="astring" value="addrconf" />
            </property_group>
        <property_group name="install_ipv4_interface" type="application"><propval name="default_route" type="net_address_v4" value="10.22.0.1" /><propval name="static_address" type="net_address_v4" value="10.22.0.58/24" /><propval name="name" type="astring" value="net1/v4" /><propval name="address_type" type="astring" value="static" /></property_group></instance>
    </service>
    <service name="network/dns/client" type="service" version="1">
        <property_group name="config" type="application">
        <property name="nameserver" type="net_address"><net_address_list><value_node value="10.22.0.2" /><value_node value="10.22.0.3" /></net_address_list></property><property name="search" type="astring"><astring_list><value_node value="vdc.opensvc.com" /><value_node value="local" /></astring_list></property></property_group>
        <instance enabled="true" name="default" />
    </service>
    <service name="system/name-service/cache" type="service" version="1">
        <instance enabled="true" name="default" />
    </service>
    <service name="system/name-service/switch" type="service" version="1">
        <property_group name="config" type="application">
        <propval name="host" type="astring" value="files dns" /><propval name="default" type="astring" value="files" /></property_group>
        <instance enabled="true" name="default" />
    </service>
    <service name="system/environment" type="service" version="1">
        <instance enabled="true" name="init">
            <property_group name="environment" type="application">
            <propval name="LANG" type="astring" value="C" /></property_group>
        </instance>
    </service>
    <service name="system/timezone" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="timezone" type="application">
                <propval name="localtime" type="astring" value="Europe/Paris" />
            </property_group>
        </instance>
    </service>
    <service name="system/config-user" type="service" version="1">
        <instance enabled="true" name="default">
            <property_group name="root_account" type="application">
                <propval name="password" type="astring" value="NP" />
                <propval name="type" type="astring" value="normal" />
                <propval name="login" type="astring" value="root" />
            </property_group>
        </instance>
    </service>
</service_bundle>'''  # nopep8


@pytest.mark.ci
class TestConfigurationProfile:
    @staticmethod
    def test_create_profile_default(tmp_file):
        sc_profile = ScProfile(sc_profile_file=tmp_file)
        sc_profile.write()
        assert os.path.exists(tmp_file)
        with open(tmp_file, 'r') as f:
            assert f.read() == EXPECTED_PROFILE_DEFAULT

    @staticmethod
    def test_create_profile_custom(tmp_file):
        sc_profile = ScProfile(sc_profile_file=tmp_file)
        sc_profile.set_nodename('skelzone-opensvc')
        sc_profile.set_localtime('Europe/Paris')
        sc_profile.add_ipv4_interface(InstallIpv4Interface('net1/v4',
                                                           static_address='10.22.0.58/24',
                                                           address_type='static',
                                                           default_route='10.22.0.1'))
        sc_profile.set_name_service_switch({'host': 'files dns'})
        sc_profile.set_environment({'LANG': 'C'})
        sc_profile.set_dns_client(searches=['vdc.opensvc.com', 'local'],
                                  nameservers=['10.22.0.2', '10.22.0.3'])
        sc_profile.write()
        sc_profile.write()
        assert os.path.exists(tmp_file)
        with open(tmp_file, 'r') as f:
            assert f.read() == EXPECTED_PROFILE_CUSTOM

    @staticmethod
    def test_create_profile_without_ip(tmp_file):
        sc_profile = ScProfile(sc_profile_file=tmp_file)
        sc_profile.add_ipv4_interface()
        sc_profile.write()
        assert os.path.exists(tmp_file)
