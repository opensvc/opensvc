import os


def edit_file(editor, fpath):
    # for python 2.7 pylint disable no-member
    os.system(' '.join((editor, fpath)))  # pylint: disable=no-member
