import json
import re
import time

from foreign.six.moves import queue
import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from utilities.naming import normalize_jsonpath
from foreign.jsonpath_ng.ext import parse
from utilities.converters import convert_boolean
from utilities.string import is_string


OPERATORS = (">=", "<=", "=", ">", "<", "~", " in ")
MAX_DURATION = 30

class Handler(daemon.handler.BaseHandler):
    """
    Wait <duration> for <condition> to become true.
    The <duration> is capped to 30 seconds.
    Upon timeout, it is up to the caller to re-submit the request until the condition becomes true.
    """
    routes = (
        ("GET", "wait"),
    )
    prototype = [
        {
            "name": "condition",
            "required": True,
            "format": "string",
            "desc": "An condition expressed as <jsonpath><operator><value>. The jsonpath is looked up in the daemon status dataset. Supported operators are %s." % " ".join(OPERATORS),
        },
        {
            "name": "duration",
            "required": False,
            "format": "duration",
            "desc": "How long to wait for the condition to become true. This duration is capped to %d seconds." % MAX_DURATION,
            "default": MAX_DURATION,
        },
    ]
    access = {
        "roles": ["guest"],
        "namespaces": "ANY",
    }

    def action(self, nodename, thr=None, stream_id=None, **kwargs):
        thr.selector = "**"
        options = self.parse_options(kwargs)
        duration = options.duration if (options.duration is not None and options.duration < MAX_DURATION) else MAX_DURATION
        timeout = time.time() + duration
        if not options.condition:
            return {"status": 0, "data": {"satisfied": True, "duration": duration, "elapsed": 0}}
        if not thr.event_queue:
            thr.event_queue = queue.Queue()
        if not thr in thr.parent.events_clients:
            thr.parent.events_clients.append(thr)
        neg, jsonpath_expr, oper, val = self.parse_condition(options.condition)
        if neg ^ self.match(jsonpath_expr, oper, val, {"kind": "patch"}, thr=thr):
            return {"status": 0, "data": {"satisfied": True, "duration": duration, "elapsed": 0}}
        end = False
        while True:
            left = timeout - time.time()
            if left < 0:
                left = 0
            try:
                msg = thr.event_queue.get(True, left if left < 3 else 3)
            except queue.Empty:
                msg = {"kind": "patch"}
                if left < 3:
                    end = True
            if neg ^ self.match(jsonpath_expr, oper, val, msg, thr=thr):
                return {"status": 0, "data": {"satisfied": True, "duration": duration, "elapsed": duration-left}}
            if end:
                return {"status": 1, "data": {"satisfied": False, "duration": duration, "elapsed": duration-left}}

    def parse_condition(self, condition):
        oper = None
        val = None

        if condition[0] == "!":
            path = condition[1:]
            neg = True
        else:
            path = condition
            neg = False

        for op in OPERATORS:
            idx = path.rfind(op)
            if idx < 0:
                continue
            val = path[idx+len(op):].strip()
            path = path[:idx].strip()
            oper = op
            if op == "~":
                if not val.startswith(".*") and not val.startswith("^"):
                    val = ".*" + val
                if not val.endswith(".*") and not val.endswith("$"):
                    val = val + ".*"
            break

        path = normalize_jsonpath(path)
        try:
            jsonpath_expr = parse(path)
        except Exception as exc:
            raise ex.Error(exc)

        return neg, jsonpath_expr, oper, val

    def eval_condition(self, jsonpath_expr, oper, val, data):
        for match in jsonpath_expr.find(data):
            if oper is None:
                if match.value:
                    return True
                else:
                    continue
            obj_class = type(match.value)
            try:
                if obj_class == bool:
                    val = convert_boolean(val)
                else:
                    val = obj_class(val)
            except Exception as exc:
                raise ex.Error("can not convert to a common type")
            if oper is None:
                if match.value:
                    return True
            if oper == "=":
                if match.value == val:
                    return True
            elif oper == ">":
                if match.value > val:
                    return True
            elif oper == "<":
                if match.value < val:
                    return True
            elif oper == ">=":
                if match.value >= val:
                    return True
            elif oper == "<=":
                if match.value <= val:
                    return True
            elif is_string(match.value) and oper == "~":
                if re.match(val, match.value):
                    return True
            elif oper == " in ":
                try:
                    l = json.loads(val)
                except:
                    l = val.split(",")
                if match.value in l:
                    return True
        return False

    def match(self, jsonpath_expr, oper, val, msg, thr=None):
        kind = msg.get("kind")
        if kind == "patch":
            if self.eval_condition(jsonpath_expr, oper, val, thr.daemon_status_data.get()):
                return True
        elif kind == "event":
            if self.eval_condition(jsonpath_expr, oper, val, msg):
                return True
        return False

