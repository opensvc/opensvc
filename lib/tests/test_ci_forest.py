from __future__ import print_function
import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

from forest import Forest, forest
from rcColor import color

class TestForest:
    def test_forest_class(self):
        """
        Forest class
        """
        tree = Forest()
        tree.load({})
        tree.print()
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
        col = node.add_column("docker container collector.container.0@registry.ope"
                              "nsvc.com/busybox:latest")
        col.add_text("warn", color.BROWN)
        col.add_text("err", color.RED)
        node = overall_node.add_node()
        node.add_column("accessory")
        node = overall_node.add_node()
        node.load("loaded text", title="loaded title")
        node = overall_node.add_node()
        node.load({"text": "loaded dict"})
        node = overall_node.add_node()
        node.load([{"text": "loaded list"}])
        buff = str(tree)
        print(buff)
        assert "loaded" in buff

    def test_forest_function(self):
        """
        Forest function
        """
        data = {
            "data": [
                {
                    "text": "node1",
                    "color": color.BOLD
                },
            ],
            "children": [
                {
                    "data": [
                        [
                            {
                                "text": "node11"
                            },
                            {
                                "text": None
                            },
                            {
                            },
                        ],
                        {
                            "text": "down",
                            "color": color.RED
                        },
                        {
                        },
                        {
                            "text": "10 MB",
                            "align": "right"
                        }
                    ],
                    "children": [
                        {
                            "data": {
                                "text": "node111",
                            }
                        }
                    ]
                },
                {
                    "data": [
                        {
                            "text": "node12 blah blah blah"
                        },
                        {
                            "text": "up",
                            "color": color.GREEN
                        },
                        {
                        },
                        {
                            "text": "10 MB",
                            "align": "right"
                        }
                    ]
                }
            ]
        }
        widths = [
            (6, 10),   # min 0, max 10 chars
            None,      # no constraints, auto detect
            None,      # no constraints, auto detect
            10         # exactly 10 chars
        ]

        buff = forest(data, columns=4, widths=widths, force_width=20)
        print(buff)
        assert "blah" in buff
        try:
            forest(data, columns=3, widths=[1])
            assert False
        except IndexError:
            pass
        except Exception:
            assert False
