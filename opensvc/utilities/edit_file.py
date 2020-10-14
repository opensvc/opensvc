import os


def edit_file(editor, fpath):
    # for some pylint disable no-member error (python 2.7)
    os.system(' '.join((editor, fpath)))  # pylint: disable=no-member
