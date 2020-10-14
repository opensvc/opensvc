"""
edit_file function
"""

import os


def edit_file(editor, fpath):
    os.system(' '.join((editor, fpath)))  # pylint: disable=no-member
