DRIVER_GROUP = "share"
DRIVER_BASENAME = "nfs"
BASE_KEYWORDS = [
    {
        "keyword": "path",
        "at": True,
        "required": True,
        "text": "The fullpath of the directory to share."
    },
    {
        "keyword": "opts",
        "at": True,
        "required": True,
        "text": "The NFS share export options, as they woud be set in /etc/exports or passed to Solaris share command."
    },
]

