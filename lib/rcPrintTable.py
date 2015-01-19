from textwrap import wrap
import sys

if sys.version_info[0] >= 3:
    from functools import reduce

def parse_data(data):
    lines = data.splitlines()
    if len(lines) < 2:
        print("no data")
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

def print_table(data, width=20, table=False):
    if table:
        from tabulate import tabulate
        try:
            print(tabulate(data, headers="firstrow", tablefmt="simple"))
        except UnicodeEncodeError:
            print(tabulate(data, headers="firstrow", tablefmt="simple").encode("utf-8"))
        return
    if not isinstance(data, list):
        data = parse_data(data)
    if len(data) < 2:
        print("no data")
    labels = data[0]
    max_label_len = reduce(lambda x,y: max(x,len(y)), labels, 0)
    data = data[1:]
    subsequent_indent = ""
    for i in range(max_label_len+4):
        subsequent_indent += " "
    for j, d in enumerate(data):
        print("-")
        for i, label in enumerate(labels):
            val = '\n'.join(wrap(convert(d[i]),
                       initial_indent = "",
                       subsequent_indent = subsequent_indent,
                       width=78
                  ))
            print(" %s = %s" % (label.ljust(max_label_len), val))
