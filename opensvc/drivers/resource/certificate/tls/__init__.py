from core.resource import DataResource
from core.objects.svcdict import KEYS

DRIVER_GROUP = "certificate"
DRIVER_BASENAME = "tls"
KEYWORDS = [
    {
        "keyword": "certificate_secret",
        "at": True,
        "text": "The name of the secret object name hosting the certificate files. The secret must have the certificate_chain and server_key keys set. This setting makes the certificate served to envoy via the secret discovery service, which allows its live rotation."
    },
    {
        "keyword": "validation_secret",
        "at": True,
        "text": "The name of the secret object name hosting the certificate autority files for certificate_secret validation. The secret must have the trusted_ca and verify_certificate_hash keys set. This setting makes the validation data served to envoy via the secret discovery service, which allows certificates live rotation."
    },
    {
        "keyword": "certificate_chain_filename",
        "at": True,
        "text": "Local filesystem data source of the TLS certificate chain."
    },
    {
        "keyword": "private_key_filename",
        "at": True,
        "text": "Local filesystem data source of the TLS private key."
    },
    {
        "keyword": "certificate_chain_inline_string",
        "at": True,
        "text": "String inlined data source of the TLS certificate chain."
    },
    {
        "keyword": "private_key_inline_string",
        "at": True,
        "text": "String inlined filesystem data source of the TLS private key. A reference to a secret for example."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class CertificateTls(DataResource):
    def __init__(self, **kwargs):
        super(CertificateTls, self).__init__(type="certificate.tls", **kwargs)
        self.label = "tls certificate"
