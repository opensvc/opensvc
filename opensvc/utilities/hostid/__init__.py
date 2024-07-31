from uuid import uuid4

def hostid():
    return uuid4().bytes[:8].hex()
