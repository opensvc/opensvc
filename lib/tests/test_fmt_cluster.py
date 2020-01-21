import pytest

from rcUtilities import abbrev


@pytest.mark.ci
@pytest.mark.parametrize(
    'input_nodes,expected_nodes', [
        [[], []],
        [['n1'], ['n1']],
        [['n1', 'n2'], ['n1', 'n2']],
        [['n1.org', 'n2'], ['n1..', 'n2']],
        [['n1.org', 'n1'], ['n1..', 'n1']],
        [['n1.org.com', 'n2.org.com'], ['n1..', 'n2..']],
        [['n1.org1.com', 'n2.org2.com'], ['n1.org1..', 'n2.org2..']],
        [['n1.org1.com', 'n2'], ['n1..', 'n2']],
        [['n1.org1.com', 'n1'], ['n1..', 'n1']],
    ]
)
def test_it_correctly_trim_nodes(input_nodes, expected_nodes):
    assert abbrev(input_nodes) == expected_nodes
