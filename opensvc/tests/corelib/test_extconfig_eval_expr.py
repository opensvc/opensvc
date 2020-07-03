import pytest

from core.extconfig import eval_expr


@pytest.mark.ci
class TestEvalExpr:
    def test_expr(self):
        """
        Arithmetic expressions eval
        """
        expressions = (
            ("1+2", 3),
            ("a+b", "ab"),
            ("a+'b'", "ab"),
            ("a in (a, b, 1)", True),
            ("a in (b, 1)", False),
            ("a == b", False),
            ("a == a", True),
            ("0 == 0", True),
            ("0 == 1", False),
            ("0 > 1", False),
            ("0 < 1", True),
            ("0 >= 0", True),
            ("1 >= 0", True),
            ("1 <= 0", False),
            ("0 <= 0", True),
            ("False and False", False),
            ("True and False", False),
            ("True and True", True),
            ("True or False", True),
            ("True or True", True),
            ("False or False", False),
            ("0 & 1", False),
        )
        for expr, expected in expressions:
            assert eval_expr(expr) == expected
