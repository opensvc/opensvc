from __future__ import print_function
from textwrap import wrap
from rcColor import color, colorize
import sys

if sys.version_info[0] >= 3:
    from functools import reduce

def parse_data(data):
    lines = data.splitlines()
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

def print_table_tabulate(data, width=20):
    try:
        validate_format(data)
    except Exception as e:
        return

    from tabulate import tabulate
    try:
        print(tabulate(data, headers="firstrow", tablefmt="simple"))
    except UnicodeEncodeError:
        print(tabulate(data, headers="firstrow", tablefmt="simple").encode("utf-8"))

def print_table_default(data):
    try:
        validate_format(data)
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
                print(colorize(fmt % (label+":"), color.LIGHTBLUE), val)
            except UnicodeEncodeError:
                print(colorize(fmt % (label+":"), color.LIGHTBLUE), val.encode("utf-8"))

def print_table_csv(data):
    try:
        validate_format(data)
    except Exception as e:
        print(e)
        return

    for d in data:
        print(";".join(map(lambda x: repr(x), d)))

