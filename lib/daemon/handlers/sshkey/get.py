import daemon.handler
import daemon.shared as shared

class Handler(daemon.handler.BaseHandler):
    """
    Return the a public key of the specified system's user, so a
    peer node can add it to its authorizations.
    """
    routes = (
        ("GET", "ssh_key"),
        (None, "ssh_key"),
    )
    prototype = [
        {
            "name": "user",
            "desc": "The system's user to fetch the public key for.",
            "format": "string",
            "required": False,
            "default": "root",
        },
        {
            "name": "key_type",
            "desc": "The type of key to retrieve.",
            "format": "string",
            "candidates": ["rsa", "dsa", "ecdsa", "ed25519"],
            "required": False,
            "default": "rsa",
        },
    ]
    access = {
        "roles": ["root"],
    }

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        data = {
            "user": options.user,
            "key": shared.NODE.get_ssh_pubkey(options.user, options.key_type),
        }
        return {"status": 0, "data": data}

