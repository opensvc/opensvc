from uuid import getnode

def hostid():
    return str(getnode())

