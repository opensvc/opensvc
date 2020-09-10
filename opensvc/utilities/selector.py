import re

import core.exceptions as ex

def selector_value_match(current, op, value):
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

def selector_config_match(svc, param, op, value):
    if not param:
        return False
    try:
        current = svc._get(param, evaluate=True)
    except (ex.Error, ex.OptNotFound, ex.RequiredOptNotFound):
        current = None
    if current is None:
        if "." in param:
            group, _param = param.split(".", 1)
        else:
            group = param
            _param = None
        rids = [section for section in svc.conf_sections() if group == "" or section.split('#')[0] == group]
        if op == ":" and len(rids) > 0 and _param is None:
            return True
        elif _param:
            for rid in rids:
                try:
                    _current = svc._get(rid+"."+_param, evaluate=True)
                except (ex.Error, ex.OptNotFound, ex.RequiredOptNotFound):
                    continue
                if selector_value_match(_current, op, value):
                    return True
        return False
    if current is None:
        return op == ":"
    if selector_value_match(current, op, value):
        return True
    return False

def selector_parse_fragment(s):
    ops = r"(<=|>=|~=|<|>|=|~|:)"
    negate = s[0] == "!"
    s = s.lstrip("!")
    elts = re.split(ops, s)
    return negate, s, elts

def selector_parse_op_fragment(elts):
    param, op, value = elts
    if op in ("<", ">", ">=", "<="):
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise ValueError
    return param, op, value

