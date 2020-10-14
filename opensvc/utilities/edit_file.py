import os


def edit_file(editor, fpath):
    os.system(' '.join((editor, fpath)))
