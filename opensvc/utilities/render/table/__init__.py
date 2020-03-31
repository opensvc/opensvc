from __future__ import print_function
from __future__ import unicode_literals

import sys

import core.exceptions as ex
import utilities.render.color
import utilities.render.term

from .tabulate import tabulate
from foreign.six.moves import reduce
from foreign.six import u as unicode
from textwrap import wrap

def parse_data(data):
    try:
        lines = data.splitlines()
    except AttributeError:
        raise ex.Error
    if len(lines) < 2:
        return []
    labels = list(map(lambda x: x.split('.')[-1], lines[0].split(',')))
    lines = lines[1:]
    rows = []
    for line in lines:
        row = []
        incell = False
        cell_begin = 0
        l = len(line)
        for i, c in enumerate(line):
            if c != ',' and i < l-1:
                continue
            if incell and ((i>1 and line[i-1] == '"') or i == l-1):
                incell = False
            if not incell:
                if i > 0:
                    if i < l-1:
                        cell = line[cell_begin:i].replace('""', '"')
                    else:
                        cell = line[cell_begin:].replace('""', '"')
                else:
                    cell = ""
                if len(cell) > 1 and cell[0] == '"' and cell[-1] == '"':
                    if len(cell) > 2:
                        cell = cell[1:-1]
                    else:
                        cell = ""
                row.append(cell)
                cell_begin = i+1
                if i<l-1 and line[i+1] == '"':
                    incell = True
        rows.append(row)
    return [labels]+rows

def convert(s):
    if isinstance(s, bool):
        s = str(s)
    try:
        return unicode(s)
    except:
        pass
    try:
        return unicode(s, errors="ignore")
    except:
        pass
    try:
        return str(s)
    except:
        pass
    return s

def validate_format(data):
    if data is None:
        raise Exception

    if not isinstance(data, list):
        data = parse_data(data)

    if len(data) == 0:
        raise Exception

    if not isinstance(data[0], list):
        for s in data:
            print(s)
        raise Exception

    if len(data) < 2:
        raise Exception

    return data

def print_table_tabulate(data, width=20):
    try:
        data = validate_format(data)
    except Exception as e:
        return

    try:
        table = tabulate(data, headers="firstrow", tablefmt="plain").splitlines()
    except UnicodeEncodeError:
        table = tabulate(data, headers="firstrow", tablefmt="plain").encode("utf-8").splitlines()

    colors = [
        utilities.render.color.color.BGWHITE + utilities.render.color.color.BLACK,
        utilities.render.color.color.E_BGODD + utilities.render.color.color.BLACK,
    ]
    idx = 0
    tw = utilities.render.term.term_width()

    for line_idx, line in enumerate(table):
        if line.startswith("+-"):
            idx = (idx + 1) % 2
            continue
        if line_idx == 0:
            color = utilities.render.color.color.BOLD
        else:
            color = colors[idx]
        if line.endswith("|"):
            cont = False
        else:
            cont = True
        length = len(line)
        if length > tw or cont:
            rpad = tw - (length % tw)
            line += " " * rpad
        print(utilities.render.color.colorize(line, color))

def print_table_default(data):
    try:
        data = validate_format(data)
    except Exception as e:
        return

    labels = data[0]
    max_label_len = reduce(lambda x,y: max(x,len(y)), labels, 0)+1
    data = data[1:]
    subsequent_indent = ""
    for i in range(max_label_len+3):
        subsequent_indent += " "
    fmt = " %-"+str(max_label_len)+"s "
    for j, d in enumerate(data):
        print("-")
        for i, label in enumerate(labels):
            val = '\n'.join(wrap(convert(d[i]),
                       initial_indent = "",
                       subsequent_indent = subsequent_indent,
                       width=78
                  ))
            try:
                print(utilities.render.color.colorize(fmt % (label+":"), utilities.render.color.color.LIGHTBLUE), val)
            except UnicodeEncodeError:
                print(utilities.render.color.colorize(fmt % (label+":"), utilities.render.color.color.LIGHTBLUE), val.encode("utf-8"))

def print_table_csv(data):
    try:
        data = validate_format(data)
    except ex.Error:
        raise ex.Error("unsupported format for this action")

    for d in data:
        print(";".join(map(lambda x: "'%s'" % unicode(x), d)))

