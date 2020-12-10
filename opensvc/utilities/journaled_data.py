from __future__ import print_function

import copy
import json
import time
import threading
import operator

import core.exceptions as ex

from functools import reduce

class JournaledDataView(object):
    def __init__(self, data=None, path=None):
        self.data = data
        self.path = path

    def exists(self, path=None):
        path = self.path + (path or [])
        return self.data.exists(path=path)

    def keys(self, path=None):
        path = self.path + (path or [])
        return self.data.keys(path=path)

    def keys_safe(self, path=None):
        path = self.path + (path or [])
        return self.data.keys_safe(path=path)

    def get(self, path=None, default=Exception):
        path = self.path + (path or [])
        return self.data.get(path=path, default=default)

    def merge(self, path=None, value=None):
        path = self.path + (path or [])
        return self.data.merge(path=path, value=value)

    def setnx(self, path=None, value=None):
        path = self.path + (path or [])
        return self.data.setnx(path=path, value=value)

    def set(self, path=None, value=None):
        path = self.path + (path or [])
        return self.data.set(path=path, value=value)

    def unset_safe(self, path=None):
        path = self.path + (path or [])
        return self.data.unset_safe(path=path)

    def unset(self, path=None):
        path = self.path + (path or [])
        return self.data.unset(path=path)

    def view(self, path=None):
        path = self.path + (path or [])
        return self.data.view(path=path)

    def inc(self, path=None):
        path = self.path + (path or [])
        return self.data.inc(path=path)

    def patch(self, path=None, patchset=None):
        path = self.path + (path or [])
        return self.data.patch(path=path, patchset=patchset)

def debug(m):
    def fn(*args, **kwargs):
        try:
            return m(*args, **kwargs)
        except:
            print(m, args, kwargs)
            import traceback
            traceback.print_stack()
            raise
    return fn

class JournaledData(object):
    def __init__(self, initial_data=None, journal_head=None,
                 journal_exclude=None, journal_condition=None,
                 event_q=None, emit_interval=0.3):
        ok = lambda: True
        self.data = initial_data or {}
        self.journal_head = journal_head
        self.journal_exclude = journal_exclude or []
        self.journal_condition = journal_condition or ok
        self.event_q = event_q
        self.emit_interval = emit_interval
        self.diff = []
        self.patch_id = 0
        self.last_emit = 0
        self.coalesce = []
        if journal_head is not None:
            self.journal_head_length = len(self.journal_head)
        #import utilities.dbglock
        #self.lock = utilities.dbglock.Lock()
        self.lock = threading.RLock()

    def view(self, path=None):
        return JournaledDataView(data=self, path=path)

    def keys_safe(self, path=None):
        try:
            return self.keys(path=path)
        except (KeyError, TypeError):
            return []

    def keys(self, path=None):
        path = path or []
        data = self.get_ref(path, self.data)
        return list(data)

    #@debug
    def get(self, path=None, default=Exception):
        try:
            return self.get_ref(path or [], self.data)
        except (TypeError, KeyError, IndexError):
            if default == Exception:
                raise
            return default

    def exists(self, path=None):
        if not path:
            return True
        try:
            self.get_ref(path, self.data)
            return True
        except (KeyError, TypeError, IndexError):
            return False

    def get_copy(self, path=None):
        path = path or []
        with self.lock:
            data = self.get_ref(path, self.data)
            return copy.deepcopy(data)

    @staticmethod
    def get_ref(path, data):
        return reduce(operator.getitem, path, data)

    #@debug
    def merge(self, path=None, value=None):
        with self.lock:
            for k, v in value.items():
                self._set_lk(path + [k], value=v)

    def setnx(self, path=None, value=None):
        with self.lock:
            if self.exists(path):
                return
            self._set_lk(path=path, value=value)

    #@debug
    def set(self, path=None, value=None):
        with self.lock:
            self._set_lk(path=path, value=value)

    def _set_lk(self, path=None, value=None):
        """
        Set data at the specified <path> to <value>.
        If path is omitted, the whole root path value is changed.

        Record the canonical change as a json_delta formatted diff, if
        * the change is not empty
        * the path changed is parented to self.journal_head

        The recorded diff is reparented to self.journal_head.
        """
        value = copy.deepcopy(value)
        path = path or []

        try:
            current = self.get_ref(path, self.data)
        except (KeyError, IndexError, TypeError):
            absolute_diff = [[path, value]]
        else:
            absolute_diff = self._diff(current, value, prefix=path)

        if not absolute_diff:
            return

        journal_diff = self._to_journal_diff(absolute_diff)

        self._set(path, value)

        self.emit(absolute_diff)
        if journal_diff:
            self.push_diff_lk(journal_diff)

    def _set(self, path, value):
        """
        Low-level set. No journaling, no messaging.
        """
        if path:
            cursor = self.get_ref(path[:-1], self.data)
            key = path[-1]
            try:
                cursor[key] = value
            except IndexError:
                if len(cursor) == key:
                    cursor.append(value)
                else:
                    raise
        else:
            self.data = value

    def _to_journal_diff(self, absolute_diff):
        if self.journal_condition() and self.journal_head is not None:
            if not self.journal_head:
                journal_diff = absolute_diff
            else:
                journal_diff = self._filter_diff(absolute_diff, head=self.journal_head, exclude=self.journal_exclude)
        else:
            journal_diff = None
        return journal_diff

    #@debug
    def patch(self, path=None, patchset=None):
        """
        No journaling.
        """
        with self.lock:
            self._patch_lk(path=path, patchset=patchset)

    def _patch_lk(self, path=None, patchset=None):
        path = path or []
        patchset = patchset or []
        self._patch(path, patchset)
        for p in patchset:
            p[0] = path + p[0]
        self.emit(patchset)

    def _patch(self, path, patchset):
        def patch_fragment(patch):
            try:
                _path, _value = patch
                self._set(path + _path, _value)
            except ValueError:
                _path, = patch
                self._unset(path + _path)

        for i, patch in enumerate(patchset):
            try:
                patch_fragment(patch)
            except Exception as exc:
                buff = "\n"
                buff += "------------------------------- Patch Error ----------------------------------\n"
                buff += "Path:\n   %s\n" % path
                buff += "Patchset:\n"
                for _i, _patch in enumerate(patchset):
                    if i == _i:
                        buff += "=> %s\n" % _patch
                    else:
                        buff += "   %s\n" % _patch
                buff += "\n"
                buff += "Current data:\n%s\n\n" % json.dumps(self.get_ref(path, self.data), indent=4)
                import traceback
                buff += "Traceback:\n"
                buff += "".join(traceback.format_stack()[:-2])
                buff += "\nException "
                buff += "".join(traceback.format_exc())
                buff += "-" * 78 + "\n"
                raise ex.Error(buff)

    def unset_safe(self, path=None):
        path = path or []
        with self.lock:
            self._unset_safe(path)

    def _unset_safe(self, path):
        try:
            self._unset_lk(path)
        except (KeyError, IndexError, TypeError):
            pass

    #@debug
    def unset(self, path=None):
        path = path or []
        with self.lock:
            self._unset_lk(path)

    def _unset(self, path):
        """
        Low-level unset. No journaling, no messaging.
        """
        data = self.get_ref(path[:-1], self.data)
        del data[path[-1]]

    def _unset_lk(self, path):
        """
        Drop data at the specified path
        """
        self._unset(path)

        diff = [[path]]
        journal_diff = self._to_journal_diff(diff)
        self.emit(diff)
        if journal_diff:
            self.push_diff_lk(journal_diff)

    def inc(self, path=None):
        with self.lock:
            return self._inc(path=path, data=self.data)

    def _inc(self, path=None, data=None):
        path = path or []
        try:
            val = self.get_ref(path, data)
            val += 1
        except (KeyError, IndexError, TypeError):
            val = 1
        self._set_lk(path=path, value=val)
        return val

    def push_diff_lk(self, diff):
        """
        Concat a diff list to the in-flight diff list.
        """
        self.diff += copy.deepcopy(diff)

    def pop_diff(self):
        """
        Return a deep copied image of changes and reset the change log
        """
        with self.lock:
            diff = [] + self.diff
            self.diff = []
        return diff

    def dump_data(self):
        """
        Return a deep copied image of the dataset
        """
        with self.lock:
            return copy.deepcopy(self.data)

    def dump_changes(self):
        """
        Return a deep copied image of the changes
        """
        with self.lock:
            return [] + self.diff


    def emit(self, diff):
        """
        Emit an event for the data change.
        The "id" event key can be used to very the sequence of event
        is not broken.
        """
        if not self.event_q:
            return
        if not diff:
            return
        now = time.time()
        self.coalesce += copy.deepcopy(diff)
        next_emit = self.emit_interval - (now - self.last_emit)
        if next_emit > 0:
            if not self.timer:
                self.timer = threading.Timer(next_emit, self._emit)
        else:
            self._emit()

    def _emit(self):
            self.patch_id += 1
            now = time.time()
            data = {
                "kind": "patch",
                "id": self.patch_id,
                "ts": now,
                "data": [] + self.coalesce,
            }
            self.event_q.put(data)
            self.last_emit = now
            self.coalesce = []
            self.timer = None

    def _filter_diff(self, diff, head=None, exclude=[]):
        data = []
        head = head or []
        exclude = exclude or []
        head_len = len(head)

        def recurse(_diff):
            path = _diff[0]
            try:
                value = _diff[1]
            except IndexError:
                # delete diff fragment, no recursion
                if path[:head_len] == head:
                    yield [path[head_len:]]
            else:
                # add diff fragment

                if path == head:
                    # exactly head
                    yield [[], value]
                else:
                    n = len(path)
                    if path[:head_len] == head:
                        # under head
                        yield [path[head_len:], value]
                    elif hasattr(value, "items"):
                        for k, v in value.items():
                            for _ in recurse([path + [k], v]):
                                yield _
                    elif hasattr(value, "enumerate"):
                        for i, v in enumerate(value):
                            for _ in recurse([path + [k], v]):
                                yield _

        def excluded(p):
            for exc in exclude:
                if p[:len(exc)] == exc:
                    return True
            return False

        for _diff in diff:
            for _ in recurse(_diff):
                p = _[0]
                if excluded(p):
                    continue
                if not p and len(_) == 1:
                    # protect journal head from deletion
                    data.append([p, None])
                    continue
                data.append(_)

        return data

    def _diff(self, src, dst, prefix=None):
        data = []
        prefix = prefix or []
        added = []

        def recurse(d1, d2, path=None, ref=None, changes=True):
            try:
                ref_v = self.get_ref(path, d2)
            except (KeyError, IndexError, TypeError):
                yield [path, d1]
            else:
                if ref_v is None and d1 is not None:
                    yield [path, d1]
                elif isinstance(d1, dict):
                    for k, v in d1.items():
                        for _ in recurse(v, d2, path=path+[k], changes=changes):
                            yield _
                elif isinstance(d1, list):
                    if prefix+path not in added:
                        if changes:
                            iterator = enumerate(d1)
                        else:
                            iterator = reversed(list(enumerate(d1)))
                        for i, v in iterator:
                            for _ in recurse(v, d2, path=path+[i], changes=changes):
                                yield _
                elif changes and ref_v != d1:
                    yield [path, d1]

        for k, v in recurse(dst, src, [], changes=True):
            data.append([prefix+k, v])
            added.append(prefix+k)

        for k, v in recurse(src, dst, [], changes=False):
            data.append([prefix+k])

        return data

if __name__ == '__main__':
    from foreign.six.moves import queue
    q = queue.Queue()
    tests = [
        ("set", dict(path=None, value={"a": {"b": 0, "c": [1, 2], "d": {"da": ""}}})),
        ("set", dict(path=["a"], value={"b": 1, "c": [1, 2, 3], "e": {"ea": 1, "eb": 2}})),
        ("set", dict(path=["a", "b"], value=2)),
        ("set", dict(path=["a", "c"], value=[1, 2, 3, 4, 5])),
        ("set", dict(path=["a", "c"], value=[1, 3, 2, 5])),
        ("set", dict(path=["a", "c"], value=[1])),
        ("set", dict(path=["a", "d"], value=["f"])),
        ("get", dict(path=["a"])),
        ("inc", dict(path=["a", "d"])),
        ("inc", dict(path=["a", "d"])),
        ("exists", dict(path=["a", "b"])),
        ("unset", dict(path=["a", "b"])),
        ("exists", dict(path=["a", "b"])),
    ]
    def run(data):
        for fn, kwargs in tests:
            print("* %s(%s)" % (fn, ", ".join(["%s=%s" % (k,v) for k, v in kwargs.items()])))
            ret = getattr(data, fn)(**kwargs)
            if ret is not None:
                print("   => %s" % ret)
            while not q.empty():
                msg = q.get(0)
                print("   => event %s" % msg)
        print("journal: %s" % data.dump_changes())
        print("data:    %s" % data.dump_data())
        rdata = JournaledData()
        rdata.patch(patchset=data.dump_changes())
        print("copy:    %s" % rdata.data)
        print()

    print("no journaling")
    print("-------------")
    data = JournaledData(event_q=q, emit_interval=0)
    run(data)

    print("full journaling")
    print("---------------")
    data = JournaledData(journal_head=[], event_q=q, emit_interval=0)
    run(data)

    print("'a' journaling, 'a.b' excluded")
    print("------------------------------")
    data = JournaledData(journal_head=["a"], event_q=q, emit_interval=0, journal_exclude=[["b"]])
    run(data)

    print("'a.b' journaling")
    print("--------------")
    data = JournaledData(journal_head=["a", "b"], event_q=q, emit_interval=0)
    run(data)


