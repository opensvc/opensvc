# -*- coding: utf8 -*-
"""
Forest data representation module.
"""
from __future__ import print_function
from __future__ import unicode_literals

import foreign.six as six

from textwrap import wrap
from utilities.render.color import colorize, color
from utilities.render.term import term_width

LAST_NODE = "`- "
NEXT_NODE = "|- "
CONT_NODE = "|  "
CONT_LAST_NODE = "   "

def forest(data, columns=1, separator="  ", widths=None, force_width=None):
    """
    Print a nested dict structure as a tree.
    Each node is considered tabular, with cells content aligned
    and wrapped.

    The widths parameter can used to set per-column min/max or exact widths:

    widths = [
        (0, 10),   # col1: min 0, max 10 chars
        None,      # col2: no constraints, auto detect
        10         # col3: exactly 10 chars
    ]

    Example:

    {
        "data": [
            {
                "text": "node1",
                "color": color.BOLD
            }
        ],
        "children": [
            {
                "data": [
                    {
                        "text": "node2"
                    },
                    {
                        "text": "foo",
                        "color": color.RED
                    }
                ],
                "children": [
                ]
            }
        ]
    }

    would be rendered as:

    node1
    `- node2 foo

    """
    if force_width:
        twidth = force_width
    else:
        twidth = term_width() - 4

    def get_pads(data, columns, widths=None):
        """
        Analyse data length in data columns and return a list of columns length,
        with no regards to terminal width constraint.
        """
        def rpads(data, pads, depth=0, max_depth=0):
            """
            Recursion function.
            """
            if "data" in data and isinstance(data["data"], dict):
                data["data"] = [data["data"]]
            for idx, col in enumerate(data.get("data", [])):
                try:
                    _width = widths[idx]
                except IndexError:
                    _width = None
                if isinstance(_width, int):
                    pads[idx] = _width
                    continue
                if isinstance(col, dict):
                    col = [col]
                for fragment in col:
                    text = fragment.get("text", "")
                    if text is None:
                        width = 0
                    else:
                        width = len(text)
                    if width > pads[idx]:
                        pads[idx] = width
                        if isinstance(_width, (list, tuple)):
                            _min, _max = _width
                            if _min is not None and pads[idx] < _min:
                                pads[idx] = _min
                            if _max is not None and pads[idx] > _max:
                                pads[idx] = _max
            next_depth = depth + 1
            for child in data.get("children", []):
                pads, depth, max_depth = rpads(child, pads, next_depth, max_depth)
                if depth > max_depth:
                    max_depth = depth
            return pads, depth, max_depth
        if widths is None:
            widths = [None] * columns
        pads = [0] * columns
        pads, _, max_depth = rpads(data, pads)
        return pads, max_depth

    def adjust_pads(pads, columns, depth, separator):
        """
        Given the pads returned by get_pads(), distribute the term width to
        columns.
        """
        max_prefix_len = depth * 3
        width = 0
        for pad in pads:
            width += pad
        width += (columns - 1) * len(separator)
        width += (depth - 1) * 3
        oversize = width - twidth
        if oversize <= 0:
            return pads
        avg_cwidth = (twidth - len(separator) * (columns - 1)) // columns
        n_oversize = 0
        for idx, pad in enumerate(pads):
            if avg_cwidth - pad < 0:
                n_oversize += 1
        remaining_width = twidth - max_prefix_len
        for idx, pad in enumerate(pads):
            if pad <= avg_cwidth:
                remaining_width -= pad + len(separator)
        for idx, pad in enumerate(pads):
            if pad > avg_cwidth:
                pads[idx] = remaining_width // n_oversize
        #print("columns:", columns)
        #print("twidth:", twidth)
        #print("avg_cwidth:", avg_cwidth)
        #print("n_oversize:", n_oversize)
        #print("remaining_width:", remaining_width)
        #print("pads:", pads)
        return pads

    def format_prefix(lasts, n_children, subnode_idx):
        """
        Return the forest markers as a string for a line.
        """
        if not lasts:
            return ""
        buff = ""
        if subnode_idx == 0:
            # new node
            for last in lasts[:-1]:
                if last:
                    buff += CONT_LAST_NODE
                else:
                    buff += CONT_NODE
            if lasts[-1]:
                buff += LAST_NODE
            else:
                buff += NEXT_NODE
        else:
            # node continuation due to wrapping
            for last in lasts:
                if last:
                    buff += CONT_LAST_NODE
                else:
                    buff += CONT_NODE
            if n_children > 0:
                buff += CONT_NODE
            else:
                buff += CONT_LAST_NODE
        return buff

    def format_cell(text, width, textcolor, separator, align):
        """
        Format the table cell, happending the separator, coloring the text and
        applying the cell padding for alignment.
        """
        if text in ("", None):
            return " " * width + separator
        if align == "right":
            fmt = "%"+str(width)+"s"
        else:
            fmt = "%-"+str(width)+"s"
        cell = fmt % text
        if textcolor:
            cell = colorize(cell, textcolor)
        return cell + separator

    def wrapped_lines(text, width):
        """
        Return lines split by the text wrapper wrapping at <width>.
        """
        if width == 0:
            return []
        return wrap(
            text,
            initial_indent="",
            subsequent_indent="",
            width=width
        )

    def recurse(data, pads, depth, buff="", lasts=None):
        """
        Recurse the data and return the forest tree buffer string.
        """
        if lasts is None:
            lasts = []
        children = data.get("children", [])
        n_children = len(children)
        last_child = n_children - 1
        for subnode_idx, subnode in enumerate(data.get("data", [])):
            prefix = format_prefix(lasts, n_children, subnode_idx)
            prefix_len = len(prefix)
            buff += prefix
            for idx, col in enumerate(subnode):
                text = col.get("text", "")
                textcolor = col.get("color")
                align = col.get("align")
                width = pads[idx]
                if idx == 0:
                    # adjust for col0 alignment shifting due to the prefix
                    width += depth * 3 - prefix_len
                buff += format_cell(text, width, textcolor, separator, align)
            buff += "\n"
        for idx, child in enumerate(children):
            last = idx == last_child
            buff = recurse(child, pads, depth, buff, lasts=lasts+[last])
        return buff

    def wrap_data(data, pads):
        """
        Transform the data, applying the wrapping to each cell and reassembling
        the results in a tabular format.
        """
        _data = {
            "data": [],
            "children": [],
        }
        tmp = []
        max_lines = 0
        for idx, col in enumerate(data.get("data", [])):
            if isinstance(col, dict):
                col = [col]
            n_lines = 0
            lines = []
            for fragment in col:
                text = fragment.get("text", "")
                textcolor = fragment.get("color")
                align = fragment.get("align")
                if text is None:
                    text = ""
                lines += [(line, textcolor, align) for line in wrapped_lines(text, pads[idx])]
            n_lines += len(lines)
            tmp.append(lines)
            if n_lines > max_lines:
                max_lines = n_lines
        for idx in range(max_lines):
            __data = []
            for _idx in range(columns):
                try:
                    _tmp = tmp[_idx]
                except IndexError:
                    break
                try:
                    line, textcolor, align = _tmp[idx]
                except IndexError:
                    line = ""
                __data.append({
                    "text": line,
                    "color": textcolor,
                    "align": align,
                })
            _data["data"].append(__data)

        children = data.get("children", [])
        for child in children:
            _data["children"].append(wrap_data(child, pads))
        return _data

    pads, depth = get_pads(data, columns, widths=widths)
    pads = adjust_pads(pads, columns, depth, separator)
    data = wrap_data(data, pads)
    #import json
    #print(json.dumps(data, indent=4))

    return recurse(data, pads, depth)

class Column(object):
    """
    The Forest Node Column object, offering a method to add extra phrases
    to the column.
    """
    def __init__(self, node=None, idx=0):
        self.idx = idx
        self.node = node

    def add_text(self, text="", textcolor=None, align=None):
        """
        Add a phrase to this column.
        """
        if not isinstance(text, six.string_types):
            text = str(text)
        if six.PY2 and isinstance(text, str):
            text = text.decode("utf8")
        self.node.node["data"][self.idx].append({
            "text": text,
            "color": textcolor,
            "align": align,
        })

class Node(object):
    """
    The Forest Node object, offering methods to add columns to the node.
    """
    def __init__(self, head, node_id):
        self.forest = head
        self.node_id = node_id
        self.node = self.forest.get_node(node_id)

    def add_column(self, text="", textcolor=None, align=None):
        """
        Add and return a column to the node with text and color.
        Extra phrases can be added through the returned Column object.
        """
        if "data" not in self.node:
            self.node["data"] = []
        if not isinstance(text, six.string_types):
            text = str(text)
        if six.PY2 and isinstance(text, str):
            text = text.decode("utf8")
        self.node["data"].append([{
            "text": text,
            "color": textcolor,
            "align": align,
        }])
        columns = len(self.node["data"])
        if columns > self.forest.columns:
            self.forest.columns = columns
        return Column(node=self, idx=columns-1)

    def add_node(self):
        """
        Add and return a new Node, child of this node.
        """
        return self.forest.add_node(parent_id=self.node_id)

    def load(self, data, title=None):
        """
        Load data in the node.
        """
        head = self
        if title:
            head.add_column(title, color.BOLD)

        def add_list(head, _data):
            """
            Load data structured as list in the node.
            """
            for idx, val in enumerate(_data):
                leaf = head.add_node()
                leaf.add_column("[%d]" % idx)
                add_gen(leaf, val)

        def add_dict(head, _data):
            """
            Load data structured as dict in the node.
            """
            for key, val in _data.items():
                leaf = head.add_node()
                leaf.add_column(key, color.LIGHTBLUE)
                add_gen(leaf, val)

        def add_gen(head, _data):
            """
            Switch between data loaders
            """
            if isinstance(_data, list):
                add_list(head, _data)
            elif isinstance(_data, dict):
                add_dict(head, _data)
            else:
                head.add_column(str(_data))

        add_gen(head, data)

class Forest(object):
    """
    The forest object, offering methods to populate and print the tree.

    Example:

    tree = Forest()
    overall_node = tree.add_node()
    overall_node.add_column("overall")

    node = overall_node.add_node()
    node.add_column("avail")
    node.add_column()
    node.add_column("up", color.GREEN)
    node = node.add_node()
    node.add_column("res#id")
    node.add_column("....")
    node.add_column("up", color.GREEN)
    col = node.add_column("label")
    col.add_text("warn", color.BROWN)
    col.add_text("err", color.RED)

    """
    def __init__(self, separator="  ", widths=None):
        self.data = {
            "data": [],
            "children": []
        }
        self.columns = 1
        self.separator = separator
        self.widths = widths

    def out(self):
        """
        Print the forest to stdout.
        """
        buff = forest(self.data, self.columns, separator=self.separator,
                      widths=self.widths)
        try:
            print(buff)
        except Exception:
            print(buff.encode("utf8", errors="ignore"))

    def __str__(self):
        return forest(self.data, self.columns, separator=self.separator,
                      widths=self.widths)

    def get_node(self, node_id, ref_node=None):
        """
        Return the Node object identified by node_id.
        """
        if ref_node is None:
            ref_node = self.data
        else:
            ref_node = ref_node["children"][node_id[0]]
        if len(node_id) == 1:
            return ref_node
        return self.get_node(node_id[1:], ref_node)

    def add_node(self, parent_id=None):
        """
        Add a node to the forest under the node identified by parent_id.
        """
        if parent_id is None:
            parent_id = []
            parent = self.data
        else:
            parent = self.get_node(parent_id)
        if "children" not in parent:
            parent["children"] = []
        node_id = parent_id + [len(parent["children"])]
        parent["children"].append({})
        return Node(self, node_id)

    def load(self, *args, **kwargs):
        """
        Load data in the Forest object.
        """
        head = self.add_node()
        head.load(*args, **kwargs)
