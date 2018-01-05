import os
import rcExceptions as ex
from node import Node
node = Node()

def test_kwdict():
    """
    Eval the Node::kwdict lazy attr
    """
    assert isinstance(node.kwdict, object)

def test_devnull():
    """
    Eval the Node::devnull lazy attr
    """
    assert isinstance(node.devnull, int)
    assert node.devnull >= 0

def test_var_d():
    """
    Eval the Node::var_d lazy attr
    """
    assert node.var_d

def test_system():
    """
    Eval the Node::system lazy attr
    """
    assert node.system

def test_compliance():
    """
    Eval the Node::compliance lazy attr
    """
    assert node.compliance

def test_split_url_1():
    """
    split url "None"
    """
    assert node.split_url("None") == ('https', '127.0.0.1', '443', '/')

def test_split_url_2():
    """
    split url "localhost"
    """
    assert node.split_url("localhost") == ('https', 'localhost', '443', None)

def test_split_url_3():
    """
    split url "https://localhost"
    """
    assert node.split_url("https://localhost") == ('https', 'localhost', '443', None)

def test_split_url_4():
    """
    split url "https://localhost:443"
    """
    assert node.split_url("https://localhost:443") == ('https', 'localhost', '443', None)

def test_split_url_5():
    """
    split url "http://localhost:8080"
    """
    assert node.split_url("http://localhost:8080") == ('http', 'localhost', '8080', None)

def test_split_url_6():
    """
    split url "http://localhost:8080/init"
    """
    assert node.split_url("http://localhost:8080/init") == ('http', 'localhost', '8080', "init")

def test_split_url_7():
    """
    split url "http://localhost:8080/feed/default/xmlrpc"
    """
    assert node.split_url("http://localhost:8080/feed/default/xmlrpc") == ('http', 'localhost', '8080', "feed")

def test_split_url_8():
    """
    split url "http://localhost/feed/default/xmlrpc"
    """
    assert node.split_url("http://localhost/feed/default/xmlrpc") == ('http', 'localhost', '80', "feed")

def test_split_url_8():
    """
    split invalid url "http://localhost:80:80/feed/default/xmlrpc"
    """
    try:
        node.split_url("http://localhost:80:80/feed/default/xmlrpc")
        assert False
    except ex.excError:
        assert True

def test_call():
    """
    Node::call()
    """
    ret, out, err = node.call(["pwd"])
    assert ret == 0

def test_vcall():
    """
    Node::vcall()
    """
    ret, out, err = node.vcall(["pwd"])
    assert ret == 0

def test_make_temp_config():
    fpath = node.make_temp_config()
    assert fpath
    os.unlink(fpath)

