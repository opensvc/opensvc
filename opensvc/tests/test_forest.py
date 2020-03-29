import pytest

from utilities.render.color import color
from utilities.render.forest import Forest, forest


@pytest.mark.ci
class TestForest:
    @staticmethod
    def test_forest_class():
        """
        Forest class
        """
        tree = Forest()
        tree.load({})
        tree.out()
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
        assert "loaded" in buff

    @staticmethod
    def test_forest_function():
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
        assert "blah" in buff
        with pytest.raises(IndexError):
            forest(data, columns=3, widths=[1])
