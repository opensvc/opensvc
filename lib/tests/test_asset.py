from __future__ import print_function
import json
import utilities.asset
from core.node import Node
import pytest


@pytest.fixture(scope='function')
def node():
    return Node()


class TestAsset:
    @staticmethod
    def test_011_get_connect_to(node):
        """
        asset connect_to on GCE, valid data
        """
        data_s = json.dumps({
            "networkInterfaces": [
                {
                    "accessConfigs": [
                        {
                            "kind": "compute#accessConfig",
                            "name": "external-nat",
                            "natIP": "23.251.137.71",
                            "type": "ONE_TO_ONE_NAT"
                        }
                    ],
                    "name": "nic0",
                    "networkIP": "10.132.0.2",
                }
            ]
        })
        asset = utilities.asset.Asset(node)
        ret = asset._parse_connect_to(data_s)
        assert ret == "23.251.137.71"

    @staticmethod
    def test_012_get_connect_to(node):
        """
        asset connect_to on GCE, empty data
        """
        data_s = json.dumps({
            "networkInterfaces": [
            ]
        })
        asset = utilities.asset.Asset(node)
        ret = asset._parse_connect_to(data_s)
        assert ret is None

    @staticmethod
    def test_013_get_connect_to(node):
        """
        asset connect_to on GCE, corrupt data
        """
        data_s = "{corrupted}"
        asset = utilities.asset.Asset(node)
        ret = asset._parse_connect_to(data_s)
        assert ret is None
