"""
Secret management command actions and options
"""
import commands.mgr.parser as mp
from utilities.optparser import OptParser, Option
from utilities.storage import Storage
from core.objects.svc import ACTION_ASYNC

PROG = "om sec"

OPT = Storage()
OPT.update(mp.OPT)
OPT.update({
    "key": Option(
        "--key", default=None,
        action="store", dest="key",
        help="The secret key name."),
    "value_from": Option(
        "--from", default=None,
        action="store", dest="value_from",
        help="Read the secret value from a file or a directory. If set to '-' or '/dev/stdin', the value is read from stdin, and the --key is mandatory. If set to a file path, the key name is the file basename. If set to a directory, one key per file is added, and the keyname is the relative path, the --key value being used as the relative path prefix."),
    "password": Option(
        "--password", default=None,
        action="store", dest="password",
        help="The pkcs bundle encryption password."),
    "path": Option(
        "--path", default=None,
        action="store", dest="path",
        help="The path where to install secret keys."),
    "value": Option(
        "--value", default=None,
        action="store", dest="value",
        help="The secret value."),
})

ACTIONS = Storage()
ACTIONS.update(mp.ACTIONS)
ACTIONS.update({
    "Secret object actions": {
        "add": {
            "msg": "Add a secret key/value to the secret object.",
            "options": mp.ACTION_OPTS + [
                OPT.value_from,
                OPT.key,
                OPT.value,
            ],
        },
        "change": {
            "msg": "Add a key/value to the configuration object. The key is created if it doesn't already exists.",
            "options": mp.ACTION_OPTS + [
                OPT.value_from,
                OPT.key,
                OPT.value,
            ],
        },
        "append": {
            "msg": "Append data to a secret key in the object.",
            "options": mp.ACTION_OPTS + [
                OPT.value_from,
                OPT.key,
                OPT.value,
            ],
        },
        "edit": {
            "msg": "Edit the configuration or the current value of a key.",
            "options": mp.ACTION_OPTS + [
                OPT.key,
            ],
        },
        "keys": {
            "msg": "Show all keys available in this secret.",
        },
        "gen_cert": {
            "msg": "Create a x509 certificate using information in the secret configuration.",
        },
        "fullpem" : {
            "msg": "Print to stdout a ascii pem-formatted concatenation of the private key and certificate. This format is accepted by opensvc context configuration. If certificate and private key are not generated yet, run the gen_cert action.",
        },
        "pkcs12" : {
            "msg": "Print to stdout a binary pkcs12-formatted concatenation of the private key and certificate. This format is accepted by most browsers certificate store. If certificate and private key are not generated yet, run the gen_cert action. A password is prompted if not already provided by --password.",
            "options": [
                OPT.password,
            ],
        },
        "decode": {
            "msg": "Decode a secret key from the secret object.",
            "options": mp.ACTION_OPTS + [
                OPT.key,
            ],
        },
        "install": {
            "msg": "Install or update secret key or secret tree in consuming volumes.",
            "options": mp.ACTION_OPTS + [
                OPT.key,
            ],
        },
        "remove": {
            "msg": "Remove a secret key from the secret object.",
            "options": mp.ACTION_OPTS + [
                OPT.key,
            ],
        },
    },
})

DEPRECATED_OPTIONS = [
]

DEPRECATED_ACTIONS = [
]

ACTIONS_TRANSLATIONS = {
}

class SecOptParser(OptParser):
    """
    The secret management command options parser class.
    """
    def __init__(self, args=None, colorize=True, width=None, formatter=None,
                 indent=6):
        OptParser.__init__(self, args=args, prog=PROG, options=OPT,
                           actions=ACTIONS,
                           deprecated_options=DEPRECATED_OPTIONS,
                           deprecated_actions=DEPRECATED_ACTIONS,
                           actions_translations=ACTIONS_TRANSLATIONS,
                           global_options=mp.GLOBAL_OPTS,
                           svc_select_options=mp.SVC_SELECT_OPTS,
                           colorize=colorize, width=width,
                           formatter=formatter, indent=indent, async_actions=ACTION_ASYNC)

