import re

def object_selector_value_match(current, op, value):
    if op in ("<", ">", ">=", "<="):
        try:
            current = float(current)
        except (ValueError, TypeError):
            return False
    if op == "=":
        if str(current).lower() in ("true", "false"):
            match = str(current).lower() == value.lower()
        else:
            match = current == value
    elif op == "~=":
        if isinstance(current, (set, list, tuple)):
            match = value in current
        else:
            try:
                match = re.search(value, current)
            except TypeError:
                match = False
    elif op == "~":
        if isinstance(current, (set, list, tuple)):
            match = any([True for v in current if re.search(value, v)])
        else:
            try:
                match = re.search(value, current)
            except TypeError:
                match = False
    elif op == ">":
        match = current > value
    elif op == ">=":
        match = current >= value
    elif op == "<":
        match = current < value
    elif op == "<=":
        match = current <= value
    elif op == ":":
        match = True
    else:
        # unknown op value
        match = False
    return match


