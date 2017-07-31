# -*- coding: utf8 -*-
"""
Forest data representation module.
"""
from __future__ import print_function
from __future__ import unicode_literals

from textwrap import wrap

from rcUtilities import term_width
from rcColor import color, colorize

LAST_NODE = "`- "
NEXT_NODE = "|- "
CONT_NODE = "|  "
CONT_LAST_NODE = "   "

def forest(data, columns=1, separator="  "):
    """
    Print a nested dict structure as a tree.
    Each node is considered tabular, with cells content aligned
    and wrapped.

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
    def get_pads(data, columns):
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
                if isinstance(col, dict):
                    col = [col]
                for fragment in col:
                    text = fragment.get("text", "")
                    width = len(text)
                    if width > pads[idx]:
                        pads[idx] = width
            next_depth = depth + 1
            for child in data.get("children", []):
                pads, depth, max_depth = rpads(child, pads, next_depth, max_depth)
                if depth > max_depth:
                    max_depth = depth
            return pads, depth, max_depth
        pads = [0] * columns
        pads, _, max_depth = rpads(data, pads)
        return pads, max_depth

    def adjust_pads(pads, columns, depth, separator):
        """
        Given the pads returned by get_pads(), distribute the term width to
        columns.
        """
        twidth = term_width() - 4
        max_prefix_len = depth * 3
        width = 0
        for pad in pads:
            width += pad
        width += (columns - 1) * len(separator)
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
        if len(lasts) == 0:
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
            if lasts[-1]:
                if n_children:
                    buff += CONT_NODE
                else:
                    buff += CONT_LAST_NODE
            else:
                buff += CONT_NODE
        return buff

    def format_cell(text, width, textcolor, separator):
        """
        Format the table cell, happending the separator, coloring the text and
        applying the cell padding for alignment.
        """
        if text in ("", None):
            return " " * width + separator
        fmt = "%-"+str(width)+"s"
        cell = fmt % text
        if textcolor:
            cell = colorize(cell, textcolor)
        return cell + separator

    def wrapped_lines(text, width):
        """
        Return lines split by the text wrapper wrapping at <width>.
        """
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
                width = pads[idx]
                if idx == 0:
                    # adjust for col0 alignment shifting due to the prefix
                    width += depth * 3 - prefix_len
                buff += format_cell(text, width, textcolor, separator)
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
                lines += [(line, textcolor) for line in wrapped_lines(text, pads[idx])]
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
                    line, textcolor = _tmp[idx]
                except IndexError:
                    line = ""
                __data.append({
                    "text": line,
                    "color": textcolor
                })
            _data["data"].append(__data)

        children = data.get("children", [])
        for child in children:
            _data["children"].append(wrap_data(child, pads))
        return _data

    pads, depth = get_pads(data, columns)
    pads = adjust_pads(pads, columns, depth, separator)
    data = wrap_data(data, pads)
    #import json
    #print(json.dumps(data, indent=4))

    return recurse(data, pads, depth)

if __name__ == "__main__":
    TESTDATA = {
        "data": [
            {
                "text": "node",
                "color": color.BOLD
            }
        ],
        "children": [
            {
                "data": [
                    {
                        "text": "node"
                    },
                    {
                        "text": "up",
                        "color": color.GREEN,
                    },
                ],
                "children": [
                    {
                        "data": [
                            {
                                "text": "node"
                            },
                            {
                                "text": "foo",
                                "color": color.BLUE
                            },
                            [
                                {
                                    "text": "label",
                                },
                                {
                                    "text": "warning: this is a long warning "
                                            "message, hopefully overflowing "
                                            "the horizontal space of the "
                                            "terminal",
                                    "color": color.BROWN
                                },
                                {
                                    "text": "error: this is a long error "
                                            "message, hopefully overflowing "
                                            "the horizontal space of the terminal",
                                    "color": color.RED
                                },
                            ],
                        ],
                        "children": [
                            {
                                "data": [
                                    {
                                        "text": "node"
                                    },
                                    {
                                        "text": "foo",
                                        "color": color.BLUE
                                    },
                                    {
                                        "text": "veeeeeee",
                                    }
                                ],
                                "children": [
                                ]
                            },
                            {
                                "data": [
                                    {
                                        "text": "node"
                                    },
                                    {
                                        "text": "foooooooooooooooooooooooooooo"
                                                "ooooooooooooooooooooooooooooo",
                                        "color": color.BLUE
                                    },
                                    {
                                        "text": "veeeeeee",
                                        "color": color.RED
                                    }
                                ],
                                "children": [
                                ]
                            }
                        ]
                    },
                    {
                        "data": [
                            {
                                "text": "node"
                            },
                            {
                                "text": "fooooooooooooooooooooooooooooooooooo"
                                        "oooooooooooooooooooooo",
                                "color": color.BLUE
                            },
                            {
                                "text": "veeeeeee",
                                "color": color.RED
                            }
                        ],
                        "children": [
                        ]
                    }
                ]
            },
            {
                "data": [
                    {
                        "text": "node"
                    },
                    {
                        "text": "foo",
                        "color": color.BLUE
                    },
                    {
                        "text": "veeeeeeeeeeeeeeeeerrrrrrrrrrrrrrrrrrrrrrrrrr"
                                "rrrrrrrrrrrrrrrrrrrrryyyyyyyyyyyyyyyllllllll"
                                "lllllllonggggggggggggggggggggggggg textttttt"
                                "tttttttt with small words too. and phrase.\n"
                                "and newlines.\nand unicodes bêèh.",
                        "color": color.RED
                    }
                ],
                "children": [
                    {
                        "data": [
                            {
                                "text": "node"
                            },
                            {
                                "text": "foo",
                                "color": color.BLUE
                            },
                            {
                                "text": "veeeeeee",
                                "color": color.RED
                            }
                        ],
                        "children": [
                        ]
                    },
                    {
                        "data": [
                            {
                                "text": "node"
                            },
                            {
                                "text": "foo",
                                "color": color.BLUE
                            },
                            {
                                "text": "veeeeeee",
                                "color": color.RED
                            }
                        ],
                        "children": [
                        ]
                    }
                ]
            }
        ]
    }
    print(forest(TESTDATA, columns=3))
