import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

from forest import Forest
from rcColor import color

class TestForest:
    def test_forest_class(self):
        """
        Forest class
        """
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
        col = node.add_column("docker container collector.container.0@registry.ope"
                              "nsvc.com/busybox:latest")
        col.add_text("warn", color.BROWN)
        col.add_text("err", color.RED)
        node = overall_node.add_node()
        node.add_column("accessory")
        print(tree)

